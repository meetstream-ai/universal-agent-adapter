"""Explicit common Deepgram ingress for public bridges using their own STT key."""

import asyncio
import hashlib
import json
import math
import time
from collections import deque
from contextlib import suppress
from uuid import uuid4

from websockets.asyncio.client import connect

from ..contract import Turn
from .protocol import decode_audio


class DeepgramInput:
    model = "nova-3"

    def __init__(self, key, on_turn, on_error, *, bot_name="Adapter SDK Test"):
        self.key, self.on_turn, self.on_error = key, on_turn, on_error
        self.bot_name = bot_name
        self.socket = None
        self.reader, self.keepalive = None, None
        self.closed = False
        self.started = time.time()
        self.id = uuid4().hex
        self.speaker = "Participant"
        self.parts = []
        self.segments = deque(maxlen=512)
        self.pending_ids = []
        self.timestamp = self.started
        self.messages_received = 0
        self.last_error = None
        self.audio_lock = asyncio.Lock()
        self.last_audio_end = time.monotonic()
        self.has_audio = False
        self.latest_word_end = -1.0
        self.latest_segment_start = -1.0

    async def start(self):
        url = (f"wss://api.deepgram.com/v1/listen?model={self.model}&encoding=linear16&sample_rate=48000&channels=1"
               "&interim_results=true&punctuate=true&endpointing=300&utterance_end_ms=2500&vad_events=true"
               "&keyterm=ADK&keyterm=CrewAI&keyterm=LangGraph&keyterm=OpenAI")
        self.socket = await connect(url, additional_headers={"Authorization": "Token " + self.key},
                                    open_timeout=10, close_timeout=2, max_size=65536, max_queue=8)
        # Establish the audio clock before a possibly lengthy meeting admission.
        await self.socket.send(bytes(9600))
        self.reader = asyncio.create_task(self._read())
        self.keepalive = asyncio.create_task(self._keepalive())

    async def audio(self, data, *, suppress_unknown=False):
        frame = decode_audio(data)
        if not frame:
            return
        _, speaker, pcm = frame
        if speaker == self.bot_name or (suppress_unknown and speaker in {"NoSpeaker", ""}):
            # Preserve the audio clock while suppressing known self attribution.
            pcm = bytes(len(pcm))
        elif speaker not in {"NoSpeaker", ""}:
            self.speaker = speaker
        if pcm:
            async with self.audio_lock, asyncio.timeout(2):
                if not self.has_audio:
                    self.started = time.time()
                    self.has_audio = True
                await self.socket.send(pcm)
                self.last_audio_end = time.monotonic() + len(pcm) / 96000

    async def result(self, message):
        """Accumulate final segments; a current 2.5-second boundary commits the turn.

        Provider endpointing can mark a pause inside a sentence speech_final.
        Executing at that boundary lets the remainder cancel its own answer.
        """
        kind = message.get("type")
        self.messages_received += 1
        if kind == "Results":
            alternatives = message.get("channel", {}).get("alternatives", [])
            words = alternatives[0].get("words", []) if alternatives else []
            if words:
                end = float(words[-1]["end"])
                if not math.isfinite(end) or end < 0:
                    raise ValueError("Invalid transcript word timing")
                start = float(message.get("start", 0))
                if not math.isfinite(start) or start < 0:
                    raise ValueError("Invalid transcript segment timing")
                # A final result can revise its interim word ending backwards.
                # Keep later segments protected, but replace the same segment's
                # provisional timing so its real boundary can commit the turn.
                if start >= self.latest_segment_start:
                    self.latest_segment_start = start
                    self.latest_word_end = end
        if kind == "Results" and message.get("is_final"):
            alternatives = message.get("channel", {}).get("alternatives", [])
            text = alternatives[0].get("transcript", "") if alternatives else ""
            segment = f"{message.get('start')}:{message.get('duration')}:{text}"
            if text and segment not in self.segments:
                self.segments.append(segment)
                self.pending_ids.append(segment)
                self.parts.append(text)
                self.timestamp = self.started + float(message.get("start", 0)) + float(message.get("duration", 0))
                if sum(len(p) for p in self.parts) > 16000:
                    raise ValueError("Transcript exceeds limit")
        if kind == "UtteranceEnd":
            boundary = float(message.get("last_word_end", -1))
            # Deepgram may announce an earlier gap while new speech already
            # appears in interim results. -1 is an explicit stale notification.
            if not math.isfinite(boundary) or boundary < 0 or boundary + 0.05 < self.latest_word_end:
                return
            if self.parts:
                text = " ".join(self.parts)
                event = hashlib.sha256((self.id + ":" + ":".join(self.pending_ids)).encode()).hexdigest()
                self.parts, self.pending_ids = [], []
                self.on_turn(Turn(event_id=event, text=text, speaker=self.speaker, timestamp=self.timestamp))
        if kind == "Error":
            raise ValueError("STT provider reported an error")

    async def _read(self):
        try:
            async for raw in self.socket:
                await self.result(json.loads(raw))
            if not self.closed:
                self.last_error = "stream_closed"
                await self.on_error()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self.closed:
                self.last_error = "read:" + type(exc).__name__
                await self.on_error()

    async def _keepalive(self):
        last_keepalive = time.monotonic()
        try:
            while True:
                await asyncio.sleep(0.25)
                # MeetStream can stop emitting mixed frames during silence.
                # KeepAlive alone cannot advance STT's utterance boundary.
                async with self.audio_lock, asyncio.timeout(2):
                    now = time.monotonic()
                    if self.has_audio and now >= self.last_audio_end + 0.25:
                        await self.socket.send(bytes(24000))
                        self.last_audio_end = time.monotonic()
                    elif not self.has_audio and now - last_keepalive >= 4:
                        await self.socket.send(json.dumps({"type": "KeepAlive"}))
                        last_keepalive = now
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self.closed:
                self.last_error = "keepalive:" + type(exc).__name__
                await self.on_error()

    async def close(self):
        self.closed = True
        for task in [self.reader, self.keepalive]:
            if task and task is not asyncio.current_task():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        if self.socket:
            await self.socket.close()
