import base64
import hashlib
import json
import re
import struct
import time
from array import array
from datetime import datetime, timezone

from ..contract import AdapterError, Turn


def runtime_switch_request(text):
    """Return the requested name only for an explicit, whole-utterance control."""
    match = re.fullmatch(r"(?:please\s+)?switch(?:ed)?\s+(?:(?:the\s+)?(?:agent|run\s*time|provider)\s+)?to\s+(.+?)[.!?]*", text.strip(), re.I | re.S)
    return re.sub(r"[\s.-]+", "", match[1].lower()) if match else None


def runtime_from_speech(text):
    """Resolve known names and observed ASR variants within a switch command."""
    name = runtime_switch_request(text)
    return {"openai": "openai", "openaiagentssdk": "openai", "adk": "adk", "googleadk": "adk",
            "adeekay": "adk", "adkay": "adk", "gribbleadk": "adk",
            "langgraph": "langgraph", "linegraph": "langgraph", "landgraph": "langgraph",
            "crewai": "crewai", "creweye": "crewai", "crewi": "crewai"}.get(name)


def meeting_ended(payload):
    event = payload.get("bot_event") or payload.get("event")
    return event in {"bot.stopped", "bot.kicked", "bot.denied", "bot.notallowed", "bot.failed"} or (
        payload.get("status") or payload.get("bot_status")) in {"Stopped", "NotAllowed", "Denied", "Done", "MediaProcessing", "MediaExpired"}


def acknowledgement(bot_id, *, audio=False):
    return {"type" if audio else "command": "ack", "bot_id": bot_id}


def legacy_audio(data):
    """Normalize MeetStream's documented JSON PCMChunk into the binary format."""
    if data.get('type') != 'PCMChunk':
        return None
    encoded_audio = data.get('audioData', '')
    if not isinstance(encoded_audio, str) or len(encoded_audio) > 350000:
        raise ValueError('Invalid PCMChunk audio')
    pcm = base64.b64decode(encoded_audio, validate=True)
    names = [data.get('speakerId') or data.get('speakerName', ''), data.get('speakerName', '')]
    if (len(pcm) > 262144 or len(pcm) % 2 or any(not isinstance(name, str) for name in names)
            or data.get('sample_rate', 48000) != 48000 or data.get('channels', 1) != 1):
        raise ValueError('Invalid PCMChunk')
    frame = bytearray(b'\x01')
    for name in names:
        encoded = name.encode()
        if len(encoded) > 4096:
            raise ValueError('Audio identity too long')
        frame.extend(len(encoded).to_bytes(2, 'little'))
        frame.extend(encoded)
    frame.extend(pcm)
    return bytes(frame)


def transcript_turn(payload, bot_id: str) -> Turn | None:
    if payload.get("bot_id") != bot_id:
        raise AdapterError("authentication_failed", "Bot binding mismatch")
    if payload.get("end_of_turn") is not True:
        return None
    text = payload.get("utterance") or payload.get("transcript") or payload.get("new_text")
    if not isinstance(text, str) or not text.strip():
        return None
    timestamp = payload.get("timestamp")
    if isinstance(timestamp, str):
        try:
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            timestamp = dt.replace(tzinfo=timezone.utc).timestamp() if dt.tzinfo is None else dt.timestamp()
        except ValueError as exc:
            raise AdapterError("invalid_input", "Invalid transcript timestamp") from exc
    if not isinstance(timestamp, (float, int)) or isinstance(timestamp, bool):
        # Arrival time cannot distinguish a repeated utterance from a duplicate delivery.
        raise AdapterError("invalid_input", "Transcript requires a stable timestamp")
    speaker = payload.get("speakerName") or payload.get("speaker") or "Participant"
    event_id = payload.get("id") or payload.get("event_id") or hashlib.sha256(
        json.dumps([bot_id, speaker, timestamp, text], sort_keys=True).encode()).hexdigest()
    return Turn(event_id=str(event_id), text=text.strip(), speaker=str(speaker), timestamp=timestamp,
                speaker_id=payload.get("speaker_id"))


class TurnPolicy:
    def __init__(self, wake_words=("hey assistant", "hey bot"), followup_seconds=30):
        # ASR can join a spoken wake phrase ("Heybot"). Keep the full
        # phrase anchored and bounded, while allowing omitted word spacing.
        self.patterns = [re.compile(r"^\s*" + r"[\s,:!?.-]*".join(re.escape(w) for w in phrase.split()) + r"\b[\s,:!?.-]*", re.I)
                         for phrase in wake_words]
        self.window = followup_seconds
        self.last_accepted = None
        self.background = []

    def apply(self, turn: Turn) -> Turn | None:
        for pattern in self.patterns:
            match = pattern.match(turn.text)
            if match:
                text = turn.text[match.end():].strip()
                if not text:
                    return None
                self.last_accepted = turn.timestamp
                return turn.model_copy(update={"text": text})
        if self.last_accepted is not None and self.last_accepted <= turn.timestamp <= self.last_accepted + self.window:
            self.last_accepted = turn.timestamp
            return turn
        self.background.append({"role": "user", "content": f"{turn.speaker}: {turn.text}"})
        self.background = self.background[-20:]
        return None


def decode_audio(data: bytes):
    """Decode both MeetStream PCM headers; normalize mono PCM16 to 48 kHz."""
    if not data or data[0] not in {1, 4}:
        return None
    rate, offset = 48000, 1
    if data[0] == 4:
        if len(data) < 6:
            raise ValueError('Truncated audio format')
        rate, channels, offset = int.from_bytes(data[1:5], 'little'), data[5], 6
        if rate not in {8000, 16000, 24000, 32000, 44100, 48000} or channels != 1:
            raise ValueError('Unsupported incoming audio format')
    names = []
    for _ in range(2):
        if offset + 2 > len(data):
            raise ValueError("Truncated audio metadata")
        size = int.from_bytes(data[offset:offset + 2], "little")
        offset += 2
        if offset + size > len(data):
            raise ValueError("Truncated speaker identity")
        names.append(data[offset:offset + size].decode("utf-8"))
        offset += size
    pcm = data[offset:]
    if len(pcm) % 2:
        raise ValueError("Incomplete PCM sample")
    if len(pcm) > rate * 2 * 3:
        raise ValueError('Audio frame exceeds three seconds')
    if pcm and rate != 48000:
        samples = array('h')
        samples.frombytes(pcm)
        import sys
        if sys.byteorder != 'little':
            samples.byteswap()
        converted = array('h')
        for index in range(len(samples) * 48000 // rate):
            position, fraction = divmod(index * rate, 48000)
            following = min(position + 1, len(samples) - 1)
            converted.append(round((samples[position] * (48000 - fraction) + samples[following] * fraction) / 48000))
        if sys.byteorder != 'little':
            converted.byteswap()
        pcm = converted.tobytes()
    return *names, pcm


class SpeechGate:
    """Conservative onset detector; mixed/unknown attribution cannot prove barge-in."""

    def __init__(self, bot_name="Adapter SDK Test", threshold=700, onset_seconds=0.12):
        self.bot_name, self.threshold, self.onset_seconds = bot_name, threshold, onset_seconds
        self.voiced = 0.0
        self.triggered = False
        self.speaker = None
        self.last_frame_at = None

    def onset(self, data):
        decoded = decode_audio(data)
        if not decoded:
            return False
        sid, name, pcm = decoded
        now = time.monotonic()
        # Mixed audio can omit silence entirely. A missing-frame gap must
        # release the previous onset, but a one-second capture is not a gap.
        if self.last_frame_at is not None and now - self.last_frame_at > max(0.5, len(pcm) / 96000 + 0.25):
            self.voiced, self.triggered = 0.0, False
        self.last_frame_at = now
        if sid != self.speaker:
            self.voiced, self.triggered, self.speaker = 0.0, False, sid
        if name in {self.bot_name, "NoSpeaker", ""} or not pcm:
            self.voiced, self.triggered = 0.0, False
            return False
        detected = False
        # Capture frames can exceed a second. Inspect 20 ms windows so silence
        # neither dilutes a short onset nor disappears inside a voiced frame.
        for offset in range(0, len(pcm), 1920):
            window = pcm[offset:offset + 1920]
            power = sum(sample[0] ** 2 for sample in struct.iter_unpack("<h", window)) / (len(window) / 2)
            if power < self.threshold ** 2:
                self.voiced, self.triggered = 0.0, False
                continue
            self.voiced += len(window) / 96000
            if self.voiced >= self.onset_seconds and not self.triggered:
                self.triggered = detected = True
        return detected
