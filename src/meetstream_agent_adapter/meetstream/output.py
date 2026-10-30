import asyncio
import base64
import json
import os
import re
import time
from contextlib import aclosing

import httpx

from ..contract import AdapterError, EventType


def pcm_48k(pcm: bytes) -> bytes:
    """2x PCM16 sample-hold resampling for the meeting's 48 kHz playback clock."""
    if len(pcm) % 2:
        raise AdapterError("invalid_input", "Incomplete PCM sample")
    output = bytearray(len(pcm) * 2)
    for offset in range(0, len(pcm), 2):
        output[offset * 2:offset * 2 + 4] = pcm[offset:offset + 2] * 2
    return bytes(output)


async def raw_pcm(chunks):
    """Apply one PCM chunking/alignment/duration policy to every provider."""
    buffer, total = bytearray(), 0
    async with aclosing(chunks):
        async for data in chunks:
            total += len(data)
            if total > 24000 * 2 * 30:
                raise AdapterError("deadline_exceeded", "Speech exceeded its duration limit")
            buffer.extend(data)
            # Two-second PCM packets avoid playback underruns between small
            # messages on a high-latency meeting control connection.
            while len(buffer) >= 96000:
                yield bytes(buffer[:96000])
                del buffer[:96000]
    if len(buffer) % 2 or not total:
        raise AdapterError("invalid_input", "Speech returned incomplete or empty PCM")
    if buffer:
        yield bytes(buffer)


class Speech:
    """OpenAI-compatible streaming PCM speech; credentials never enter adapter tools."""

    def __init__(self, api_key: str, model="tts-1", voice="alloy", base_url="https://api.openai.com/v1"):
        self.client = httpx.AsyncClient(base_url=base_url, timeout=30,
                                       headers={"Authorization": f"Bearer {api_key}"})
        self.model, self.voice = model, voice

    async def pcm(self, text):
        async with self.client.stream("POST", "/audio/speech", json={"model": self.model, "voice": self.voice,
                                      "input": text, "response_format": "pcm"}) as response:
            response.raise_for_status()
            async for pcm in raw_pcm(response.aiter_bytes()):
                yield pcm

    async def close(self):
        await self.client.aclose()


class DeepgramSpeech(Speech):
    def __init__(self, api_key, model="aura-2-thalia-en"):
        self.client = httpx.AsyncClient(base_url="https://api.deepgram.com/v1/", timeout=30,
                                       transport=httpx.AsyncHTTPTransport(retries=2),
                                       headers={"Authorization": "Token " + api_key})
        self.model = model

    async def pcm(self, text):
        async with self.client.stream("POST", "speak", params={"model": self.model, "encoding": "linear16",
                                       "sample_rate": 24000, "container": "none"}, json={"text": text}) as response:
            response.raise_for_status()
            if response.headers.get("content-type", "").split(";")[0] != "audio/l16":
                raise AdapterError("invalid_input", "Speech returned unsupported audio format")
            async for pcm in raw_pcm(response.aiter_bytes()):
                yield pcm


class GeminiSpeech:
    """Independent shared speech stage; no framework-specific voice coupling."""

    def __init__(self, api_key, model="gemini-3.8-flash-tts", voice="Kore"):
        self.client = httpx.AsyncClient(base_url="https://generativelanguage.googleapis.com/v1beta/", timeout=30,
                                       headers={"x-goog-api-key": api_key})
        self.model, self.voice = model, voice

    async def pcm(self, text):
        config = {"responseModalities": ["AUDIO"], "speechConfig": {"voiceConfig": {"voice": self.voice}}}
        body = {"contents": [{"role": "user", "parts": [{"text": text}]}], "generationConfig": config}
        async with self.client.stream("POST", f"models/{self.model}:streamGenerateContent", params={"alt": "sse"}, json=body) as response:
            response.raise_for_status()
            async for pcm in raw_pcm(self._audio_bytes(response)):
                yield pcm

    async def _audio_bytes(self, response):
        async for line in response.aiter_lines():
            if not line.startswith("data:"):
                continue
            chunk = json.loads(line[5:])
            for candidate in chunk.get("candidates", []):
                for part in candidate.get("content", {}).get("parts", []):
                    blob = part.get("inlineData")
                    if not blob:
                        continue
                    mime = blob.get("mimeType", "").lower()
                    if not mime.startswith("audio/l16") or ("rate=" in mime and "rate=24000" not in mime):
                        raise AdapterError("invalid_input", "Speech returned unsupported audio format")
                    yield base64.b64decode(blob["data"], validate=True)

    async def close(self):
        await self.client.aclose()


SPEECH_KEYS = {"openai": "OPENAI_API_KEY", "gemini": "GOOGLE_API_KEY", "deepgram": "DEEPGRAM_API_KEY"}


def create_speech(settings):
    key = os.getenv("SPEECH_API_KEY") or os.getenv(SPEECH_KEYS[settings.speech_provider])
    if not key:
        raise AdapterError("authentication_failed", "Speech credentials are missing")
    if settings.speech_provider == "gemini":
        return GeminiSpeech(key, settings.speech_model, settings.speech_voice)
    if settings.speech_provider == "deepgram":
        return DeepgramSpeech(key, settings.speech_model)
    return Speech(key, settings.speech_model, settings.speech_voice, settings.speech_base_url)


class MeetingOutput:
    def __init__(self, bot_id, speech=None, *, chat=True, sender=None):
        self.bot_id, self.speech, self.chat = bot_id, speech, chat
        self.socket = None
        self.sender = sender
        self.send_lock = asyncio.Lock()
        self.queue = asyncio.Queue(maxsize=8)
        self.worker = None
        self.buffer = ""
        self.delivery = "idle"
        self.error = None
        self.error_details = None
        self.audio_frames = 0
        self.submitted_audio_seconds = 0.0
        self.chat_messages = 0
        self.last_audio_at = 0.0
        self.resume_id = None

    def echo_possible(self):
        return self.delivery_pending() or time.monotonic() - self.last_audio_at < 0.75

    async def send(self, command, active=lambda: True):
        async with self.send_lock:
            if not active():
                return
            command["bot_id"] = self.bot_id
            async with asyncio.timeout(2):
                if self.sender:
                    await self.sender(command)
                elif self.socket:
                    await self.socket.send_json(command)
                else:
                    raise AdapterError("transport_lost", "Meeting control channel is unavailable")

    async def event(self, event, active):
        if not active():
            return
        if self.error:
            raise AdapterError("transport_lost", "Speech output failed")
        if event.type == EventType.STARTED:
            self.resume_id = event.turn_id
        if event.type == EventType.DELTA and self.speech:
            self.buffer += event.payload["text"]
            while self.buffer:
                match = re.search(r"[.!?](?:\s|$)", self.buffer)
                end = match.end() if match and match.end() <= 120 else 0
                if not end and len(self.buffer) >= 120:
                    # Numeric text expands substantially when spoken. Bound
                    # long sentences too, and keep words/numbers intact.
                    boundary = max(self.buffer.rfind(" ", 0, 120), self.buffer.rfind(",", 0, 120))
                    end = boundary + 1 if boundary > 0 else 120
                if not end:
                    break
                self._enqueue(self.buffer[:end], active)
                self.buffer = self.buffer[end:]
        if event.type == EventType.MESSAGE:
            if self.speech and self.buffer.strip():
                self._enqueue(self.buffer, active)
            self.buffer = ""
            text = event.payload.get("text", "")
            if self.chat and text:
                await self.send({"command": "sendmsg", "message": text, "msg": text}, active)
                self.chat_messages += 1
            if not self.speech and self.chat:
                self.delivery = "submitted_unconfirmed"

    def _enqueue(self, text, active):
        try:
            self.queue.put_nowait((text, active))
        except asyncio.QueueFull as exc:
            raise AdapterError("deadline_exceeded", "Speech queue is full") from exc
        if not self.worker or self.worker.done():
            self.worker = asyncio.create_task(self._speak())

    async def _speak(self):
        self.delivery = "streaming"
        resumed = False
        try:
            while not self.queue.empty():
                text, active = self.queue.get_nowait()
                try:
                    if not active():
                        continue
                    deadline = time.monotonic()
                    # Joined identifiers can be mispronounced (GammaPilot).
                    # Only speech gets word boundaries; events/chat/tools keep the original.
                    spoken = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", text)
                    async for pcm in self.speech.pcm(spoken):
                        if not active():
                            break
                        if not resumed:
                            # Zoom/Teams stay paused after interrupt; Meet ignores
                            # this command. Resume once before this turn's audio.
                            await self.send({"command": "resume_audio", "resume_id": self.resume_id or self.bot_id}, active)
                            resumed = True
                        audio = pcm_48k(pcm)
                        # Provider startup/stalls do not create playback time
                        # to catch up: start pacing when a chunk is available.
                        deadline = max(deadline, time.monotonic())
                        await self.send({"command": "sendaudio", "audiochunk": base64.b64encode(audio).decode(),
                                         "sample_rate": 48000, "encoding": "pcm16", "channels": 1,
                                         "endianness": "little"}, active)
                        duration = len(pcm) / 48000
                        self.audio_frames += 1
                        self.last_audio_at = time.monotonic()
                        self.submitted_audio_seconds += duration
                        deadline += duration
                        await asyncio.sleep(max(0, deadline - time.monotonic()))
                finally:
                    self.queue.task_done()
            self.delivery = "submitted_unconfirmed"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.error = "speech_or_transport_failed"
            self.error_details = {"class": type(exc).__name__,
                                  "code": exc.code if isinstance(exc, AdapterError) else None,
                                  "http_status": exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None}
            self.delivery = "failed"
            # Do not restart a failed speech stream or replay already submitted audio.
            while not self.queue.empty():
                self.queue.get_nowait()
                self.queue.task_done()

    async def interrupt(self):
        self.buffer = ""
        if self.worker and not self.worker.done():
            self.worker.cancel()
            done, _ = await asyncio.wait({self.worker}, timeout=1)
            if not done:
                self.error = "speech_cleanup_unconfirmed"
        while not self.queue.empty():
            self.queue.get_nowait()
            self.queue.task_done()
        self.delivery = "interrupted"
        # With no output channel yet, there is no submitted playback to clear.
        if self.socket or self.sender:
            await self.send({"command": "interrupt", "action": "clear_audio_queue"})

    async def close(self):
        try:
            await self.interrupt()
        finally:
            if self.speech:
                await self.speech.close()

    def delivery_pending(self):
        return bool(self.worker and not self.worker.done()) or bool(self.buffer) or not self.queue.empty()
