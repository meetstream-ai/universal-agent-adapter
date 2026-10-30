import base64
import json

import httpx
import pytest

from meetstream_agent_adapter.contract import AdapterError
from meetstream_agent_adapter.meetstream.output import DeepgramSpeech, GeminiSpeech, Speech, raw_pcm


@pytest.mark.parametrize('provider', ['openai', 'deepgram'])
async def test_raw_speech_is_bounded_pcm_and_rejects_malformed_samples(provider):
    requests = []
    content = b'\x01\x00' * 48500

    def respond(request):
        requests.append(request)
        return httpx.Response(200, headers={'content-type': 'audio/l16;rate=24000'}, content=content)

    speech = DeepgramSpeech('test-key') if provider == 'deepgram' else Speech('test-key')
    await speech.client.aclose()
    speech.client = httpx.AsyncClient(base_url='https://speech.example/v1/', transport=httpx.MockTransport(respond))
    try:
        chunks = [chunk async for chunk in speech.pcm('Synthetic test')]
        assert b''.join(chunks) == content and len(chunks) == 2
        if provider == 'deepgram':
            assert requests[0].url.params['container'] == 'none'
            assert requests[0].url.params['sample_rate'] == '24000'
        content = b'\x01'
        with pytest.raises(AdapterError):
            _ = [chunk async for chunk in speech.pcm('Synthetic test')]
        content = b''
        with pytest.raises(AdapterError):
            _ = [chunk async for chunk in speech.pcm('Synthetic test')]
    finally:
        await speech.close()


@pytest.mark.parametrize("case", ["valid", "empty", "odd", "format", "oversize"])
async def test_gemini_sse_preserves_pcm_boundaries_and_safety_limits(case):
    content = {"valid": b"\x01\x00" * 48500, "empty": b"", "odd": b"\x01",
               "format": b"\x01\x00", "oversize": bytes(24000 * 2 * 30 + 2)}[case]
    mime = "audio/mp3" if case == "format" else "audio/L16;rate=24000"
    # Split a sample across provider events to verify buffering across blobs.
    events = [{"candidates": [{"content": {"parts": [{"inlineData": {
        "mimeType": mime, "data": base64.b64encode(blob).decode()}}]}}]}
              for blob in [content[:1], content[1:]] if blob]

    def respond(request):
        body = json.loads(request.content)
        assert body["generationConfig"]["responseModalities"] == ["AUDIO"]
        assert request.url.params["alt"] == "sse"
        return httpx.Response(200, text="".join("data: " + json.dumps(event) + "\n\n" for event in events))

    speech = GeminiSpeech("fixture-key")
    await speech.client.aclose()
    speech.client = httpx.AsyncClient(base_url="https://speech.example/", transport=httpx.MockTransport(respond))
    try:
        if case == "valid":
            chunks = [chunk async for chunk in speech.pcm("Synthetic test")]
            assert b"".join(chunks) == content and [len(chunk) for chunk in chunks] == [96000, 1000]
        else:
            with pytest.raises(AdapterError) as error:
                _ = [chunk async for chunk in speech.pcm("Synthetic test")]
            assert error.value.code == ("deadline_exceeded" if case == "oversize" else "invalid_input")
    finally:
        await speech.close()


async def test_shared_pcm_chunker_closes_its_provider_iterator():
    closed = []

    async def source():
        try:
            yield bytes(96000)
            pytest.fail("Closing the consumer must not request another provider chunk")
        finally:
            closed.append(True)

    stream = raw_pcm(source())
    assert len(await anext(stream)) == 96000
    await stream.aclose()
    assert closed == [True]
