import pytest

from meetstream_agent_adapter.meetstream.transcription import DeepgramInput


async def test_stream_requests_runtime_keyterms_without_rewriting_transcripts(monkeypatch):
    import asyncio
    from urllib.parse import parse_qs, urlparse

    requests = []

    class Socket:
        async def send(self, data):
            pass

        async def close(self):
            pass

        async def __aiter__(self):
            await asyncio.Event().wait()
            yield ''

    async def connect(url, **kwargs):
        requests.append(url)
        return Socket()

    async def fail():
        pytest.fail('Unexpected transcription failure')

    monkeypatch.setattr('meetstream_agent_adapter.meetstream.transcription.connect', connect)
    stt = DeepgramInput('fixture-only-key', lambda turn: None, fail)
    try:
        await stt.start()
        query = parse_qs(urlparse(requests[0]).query)
        assert query['model'] == ['nova-3']
        assert set(query['keyterm']) == {'ADK', 'CrewAI', 'LangGraph', 'OpenAI'}
        assert 'fixture-only-key' not in requests[0]
    finally:
        await stt.close()


async def test_final_segments_commit_once_and_partials_do_not_execute():
    turns = []

    async def fail():
        pytest.fail("Unexpected STT failure")

    stt = DeepgramInput("not-a-real-key", turns.append, fail)
    stt.speaker = "Tester"
    def message(text, start, final, end=False):
        return {"type": "Results", "start": start, "duration": 1, "is_final": final, "speech_final": end,
                "channel": {"alternatives": [{"transcript": text, "words": [{"end": start + 1}]}]}}
    await stt.result(message("provisional", 0, False))
    assert not turns
    first = message("Hey assistant", 0, True)
    await stt.result(first)
    await stt.result(first)
    await stt.result(message("save an action", 1, True, True))
    assert not turns  # A short provider endpoint is not a complete participant turn.
    await stt.result({"type": "UtteranceEnd", "last_word_end": 2})
    assert len(turns) == 1 and turns[0].text == "Hey assistant save an action"
    assert turns[0].speaker == "Tester"
    await stt.result(message("a second utterance", 3, True))
    await stt.result({"type": "UtteranceEnd", "last_word_end": 4})
    assert len(turns) == 2 and turns[1].event_id != turns[0].event_id


async def test_stale_boundary_cannot_commit_a_continuing_request():
    turns = []

    async def fail():
        pytest.fail("Unexpected STT failure")

    stt = DeepgramInput("not-a-real-key", turns.append, fail)
    def segment(text, start, end, final):
        return {"type": "Results", "start": start, "duration": end - start, "is_final": final,
                "channel": {"alternatives": [{"transcript": text, "words": [{"end": end}]}]}}
    await stt.result(segment("Hey bot, create one", 0, 1, True))
    await stt.result(segment("action titled verify assigned to team", 2, 3, False))
    await stt.result({"type": "UtteranceEnd", "last_word_end": 1})
    assert not turns
    await stt.result(segment("action titled verify assigned to team", 2, 3, True))
    await stt.result({"type": "UtteranceEnd", "last_word_end": -1})
    assert not turns
    await stt.result({"type": "UtteranceEnd", "last_word_end": 3})
    assert len(turns) == 1
    assert turns[0].text == "Hey bot, create one action titled verify assigned to team"


async def test_sparse_meeting_audio_still_advances_utterance_clock():
    import asyncio
    import time

    sent = []
    received = asyncio.Event()

    class Socket:
        async def send(self, data):
            sent.append(data)
            received.set()

        async def close(self):
            pass

    async def fail():
        pytest.fail('Unexpected STT transport failure')

    stt = DeepgramInput('not-a-real-key', lambda turn: None, fail)
    stt.socket = Socket()
    stt.has_audio = True
    stt.last_audio_end = time.monotonic() - 1
    stt.keepalive = asyncio.create_task(stt._keepalive())
    try:
        async with asyncio.timeout(2):
            await received.wait()
        assert sent[0] == bytes(24000)  # Raw silence, rather than a KeepAlive-only frame.
    finally:
        await stt.close()


async def test_unknown_mixed_audio_is_transcribed_except_during_possible_self_echo():
    import struct

    sent = []

    class Socket:
        async def send(self, data):
            sent.append(data)

    async def fail():
        pytest.fail('Unexpected STT error')

    stt = DeepgramInput('not-a-real-key', lambda turn: None, fail)
    stt.socket = Socket()
    name, sid, pcm = b'NoSpeaker', b'sid', struct.pack('<h', 1400) * 100
    frame = b'\x01' + struct.pack('<H', len(sid)) + sid + struct.pack('<H', len(name)) + name + pcm
    await stt.audio(frame)
    await stt.audio(frame, suppress_unknown=True)
    assert sent == [pcm, bytes(len(pcm))]


async def test_observer_bot_does_not_suppress_the_assistant_it_is_listening_to():
    import struct

    sent = []
    class Socket:
        async def send(self, data):
            sent.append(data)
    async def error():
        pytest.fail('Unexpected observer error')
    listener = DeepgramInput('fixture-key', lambda turn: None, error, bot_name='Voice Observer')
    listener.socket = Socket()
    pcm = struct.pack('<h', 1000) * 100
    for name in ['Adapter SDK Test', 'Voice Observer']:
        encoded = name.encode()
        await listener.audio(b'\x01\x02\x00id' + len(encoded).to_bytes(2, 'little') + encoded + pcm)
    assert sent == [pcm, bytes(len(pcm))]


async def test_final_word_timing_revision_does_not_stall_or_commit_newer_speech():
    turns = []
    async def fail():
        pytest.fail("Unexpected STT error")
    stt = DeepgramInput("fixture-key", turns.append, fail)
    def result(start, end, final, text):
        return {"type": "Results", "start": start, "duration": end - start,
                "is_final": final, "channel": {"alternatives": [
                    {"transcript": text, "words": [{"end": end}]}]}}
    await stt.result(result(0, 3.4, False, "provisional"))
    await stt.result(result(0, 3.0, True, "Okay assistant, check status"))
    await stt.result({"type": "UtteranceEnd", "last_word_end": 3.0})
    assert [turn.text for turn in turns] == ["Okay assistant, check status"]
    await stt.result(result(5, 6, True, "Create"))
    await stt.result(result(7, 9, False, "an action"))
    await stt.result(result(5, 5.9, True, "Create"))
    await stt.result({"type": "UtteranceEnd", "last_word_end": 5.9})
    assert len(turns) == 1
