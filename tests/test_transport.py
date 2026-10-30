import asyncio
import struct
import time

import pytest

from meetstream_agent_adapter.contract import AdapterError, AgentEvent, EventType
from meetstream_agent_adapter.meetstream.output import MeetingOutput, pcm_48k
from meetstream_agent_adapter.meetstream.protocol import SpeechGate, TurnPolicy, decode_audio, transcript_turn


def audio(name="Tester", samples=6000, power=1800):
    sid, name = b"sid", name.encode()
    return b"\x01" + struct.pack("<H", len(sid)) + sid + struct.pack("<H", len(name)) + name + struct.pack("<h", power) * samples


def test_audio_protocol_bounds_and_gate():
    samples = struct.pack("<hh", -32768, 32767)
    assert pcm_48k(samples) == struct.pack("<hhhh", -32768, -32768, 32767, 32767)
    with pytest.raises(AdapterError):
        pcm_48k(b"\x00")
    assert decode_audio(audio())[1] == "Tester"
    gate = SpeechGate()
    assert not gate.onset(audio("Adapter SDK Test"))
    assert not gate.onset(audio("NoSpeaker"))
    assert gate.onset(audio())
    assert not gate.onset(audio())
    assert not gate.onset(audio(power=0))
    assert gate.onset(audio())
    assert decode_audio(b"\x02") is None
    for malformed in [b"\x01", b"\x01\xff\xffa", audio()[:-1]]:
        with pytest.raises(ValueError):
            decode_audio(malformed)


def test_speech_onset_recovers_when_transport_omits_silence(monkeypatch):
    clock = [1.0]
    monkeypatch.setattr("meetstream_agent_adapter.meetstream.protocol.time.monotonic", lambda: clock[0])
    gate = SpeechGate()
    assert gate.onset(audio())
    clock[0] += 0.125
    assert not gate.onset(audio())
    # Same participant, no zero-valued PCM during the pause.
    clock[0] += 0.6
    assert gate.onset(audio())
    # A normal one-second capture cadence is not a missing-frame pause.
    gate = SpeechGate()
    assert gate.onset(audio(samples=49152))
    clock[0] += 1.024
    assert not gate.onset(audio(samples=49152))


def test_speech_onset_preserves_boundaries_inside_large_frames():
    header = audio(samples=0)
    voice = struct.pack("<h", 1800) * 6000
    silence = bytes(96000)
    gate = SpeechGate()
    # A documented one-second capture may contain speech and trailing silence.
    assert gate.onset(header + voice + silence)
    assert gate.onset(header + voice + silence)
    # Silence must not dilute a short onset below the whole-frame RMS threshold.
    assert SpeechGate().onset(header + silence * 2 + voice)
    # Disconnected short noises cannot accumulate into a speech onset.
    short = struct.pack("<h", 1800) * 1920
    assert not SpeechGate().onset(header + (short + bytes(3840)) * 4)


def test_transcript_finalization_dedup_and_wake_policy():
    payload = {"bot_id": "bot", "speakerName": "Tester", "timestamp": "2026-10-08T00:00:00Z",
               "new_text": "Hey, assistant: check project", "end_of_turn": False}
    assert transcript_turn(payload, "bot") is None
    payload["end_of_turn"] = True
    t = transcript_turn(payload, "bot")
    assert transcript_turn(payload, "bot").event_id == t.event_id
    policy = TurnPolicy()
    assert policy.apply(t).text == "check project"
    assert TurnPolicy().apply(t.model_copy(update={"text": "Heybot. check project"})).text == "check project"
    assert TurnPolicy().apply(t.model_copy(update={"text": "Heybotany check project"})) is None
    assert TurnPolicy(["okay assistant"]).apply(t.model_copy(update={"text": "Okay. Assistant, check project"})).text == "check project"
    assert TurnPolicy(["okay assistant"]).apply(t.model_copy(update={"text": "Okay. Assistantship, check project"})) is None
    assert policy.apply(t.model_copy(update={"timestamp": t.timestamp + 2, "text": "follow up"})).text == "follow up"
    assert policy.apply(t.model_copy(update={"timestamp": t.timestamp + 100, "text": "background"})) is None
    with pytest.raises(AdapterError):
        transcript_turn(payload, "different")
    payload.pop("timestamp")
    with pytest.raises(AdapterError):
        transcript_turn(payload, "bot")


def event(kind, text):
    return AgentEvent(session_id="s", turn_id="t", generation=1, sequence=1, type=kind, payload={"text": text})


async def test_playback_interrupt_after_reasoning_completion():
    commands = []
    frame_sent = asyncio.Event()

    async def sender(command):
        commands.append(dict(command))
        if command["command"] == "sendaudio":
            frame_sent.set()

    class Speech:
        async def pcm(self, text):
            yield b"\x01\x00" * 6000
            await asyncio.Event().wait()

        async def close(self):
            pass

    active = True
    output = MeetingOutput("bot", Speech(), sender=sender)
    await output.event(event(EventType.STARTED, ""), lambda: active)
    await output.event(event(EventType.DELTA, "Hello."), lambda: active)
    await output.event(event(EventType.MESSAGE, "Hello."), lambda: active)
    await frame_sent.wait()
    assert output.delivery_pending()
    active = False
    await output.interrupt()
    assert output.worker.done()
    assert commands[-1]["command"] == "interrupt"
    assert sum(c["command"] == "sendaudio" for c in commands) == 1
    resume = next(c for c in commands if c["command"] == "resume_audio")
    assert resume["resume_id"] == "t"
    assert commands.index(resume) < next(i for i, c in enumerate(commands) if c["command"] == "sendaudio")
    assert next(c for c in commands if c["command"] == "sendaudio")["sample_rate"] == 48000
    assert sum(c["command"] == "sendmsg" for c in commands) == 1
    # A fresh generation resumes playback after clearing the previous one.
    active = True
    frame_sent.clear()
    next_turn = event(EventType.STARTED, "").model_copy(update={"turn_id": "next"})
    await output.event(next_turn, lambda: active)
    await output.event(event(EventType.DELTA, "Next."), lambda: active)
    await frame_sent.wait()
    assert [c["resume_id"] for c in commands if c["command"] == "resume_audio"] == ["t", "next"]
    assert sum(c["command"] == "sendaudio" for c in commands) == 2
    await output.close()


async def test_output_rechecks_generation_inside_send_lock():
    commands = []

    async def sender(command):
        commands.append(command)

    output = MeetingOutput("bot", sender=sender)
    active = True
    await output.send_lock.acquire()
    task = asyncio.create_task(output.send({"command": "sendmsg"}, lambda: active))
    await asyncio.sleep(0)
    active = False
    output.send_lock.release()
    await task
    assert not commands


async def test_speech_separates_joined_words_without_changing_chat():
    commands, spoken = [], []

    class Speech:
        async def pcm(self, text):
            spoken.append(text)
            yield bytes(240)

        async def close(self):
            pass

    async def sender(command):
        commands.append(command)

    output = MeetingOutput('bot', Speech(), sender=sender)
    text = 'GammaPilot uses CrewAI and LangGraph.'
    try:
        await output.event(event(EventType.DELTA, text), lambda: True)
        await output.event(event(EventType.MESSAGE, text), lambda: True)
        await output.worker
        assert spoken == ['Gamma Pilot uses Crew AI and Lang Graph.']
        assert next(c for c in commands if c['command'] == 'sendmsg')['message'] == text
    finally:
        await output.close()


async def test_provider_delay_does_not_burst_queued_audio():
    sent = []

    class Speech:
        async def pcm(self, text):
            await asyncio.sleep(0.08)
            yield bytes(12000)
            yield bytes(12000)

        async def close(self):
            pass

    async def sender(command):
        if command["command"] == "sendaudio":
            sent.append(time.monotonic())

    output = MeetingOutput("bot", Speech(), sender=sender)
    try:
        await output.event(event(EventType.DELTA, "Hello."), lambda: True)
        await output.worker
        assert len(sent) == 2 and sent[1] - sent[0] >= 0.24
    finally:
        await output.close()


async def test_speech_failure_is_separate_delivery_failure():
    class Speech:
        async def pcm(self, text):
            raise RuntimeError("secret")
            yield

        async def close(self):
            pass

    async def sender(command):
        pass

    output = MeetingOutput("bot", Speech(), sender=sender)
    await output.event(event(EventType.DELTA, "Hello."), lambda: True)
    await output.worker
    assert output.delivery == "failed" and output.error == "speech_or_transport_failed"
    assert output.error_details == {"class": "RuntimeError", "code": None, "http_status": None}
    await output.close()


@pytest.mark.parametrize("text", [", ".join(str(n) for n in range(1, 101)) + ".", " ".join(["framework boundary"] * 30) + "."])
async def test_long_speech_sentences_preserve_words_and_numbers_with_bounded_phrases(text):
    phrases = []

    class Speech:
        async def pcm(self, phrase):
            phrases.append(phrase)
            yield bytes(48)

        async def close(self):
            pass

    async def sender(command):
        pass

    output = MeetingOutput("bot", Speech(), sender=sender)
    try:
        await output.event(event(EventType.DELTA, text), lambda: True)
        await output.event(event(EventType.MESSAGE, text), lambda: True)
        await output.worker
        assert not output.error and "".join(phrases) == text
        assert all(len(phrase) <= 120 for phrase in phrases)
        assert all(not (left[-1].isalnum() and right[0].isalnum()) for left, right in zip(phrases, phrases[1:], strict=False))
    finally:
        await output.close()


def test_documented_legacy_pcm_audio_normalizes_and_rejects_malformed_input():
    import base64

    from meetstream_agent_adapter.meetstream.protocol import decode_audio, legacy_audio

    pcm = b'\x00\x00\x01\x00'
    frame = legacy_audio({'type': 'PCMChunk', 'speakerName': 'Participant', 'audioData': base64.b64encode(pcm).decode()})
    assert decode_audio(frame) == ('Participant', 'Participant', pcm)
    assert legacy_audio({'type': 'ack'}) is None
    for value in ['*', base64.b64encode(b'\x00').decode()]:
        with pytest.raises(ValueError):
            legacy_audio({'type': 'PCMChunk', 'audioData': value})


@pytest.mark.parametrize('rate', [16000, 24000, 32000, 44100, 48000])
def test_rate_tagged_audio_preserves_identity_duration_and_signal(rate):
    import struct

    from meetstream_agent_adapter.meetstream.protocol import decode_audio

    pcm = struct.pack('<h', 1200) * (rate // 50)
    frame = b'\x04' + rate.to_bytes(4, 'little') + b'\x01' + b'\x02\x00id\x03\x00Sam' + pcm
    identity, name, normalized = decode_audio(frame)
    assert (identity, name) == ('id', 'Sam')
    assert len(normalized) == 1920
    assert set(struct.unpack('<960h', normalized)) == {1200}
    with pytest.raises(ValueError, match='Unsupported'):
        decode_audio(frame[:5] + b'\x02' + frame[6:])
