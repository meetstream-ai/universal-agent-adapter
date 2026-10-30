import json
from types import SimpleNamespace

import pytest

from meetstream_agent_adapter.cli import doctor, load_environment, manage
from meetstream_agent_adapter.config import (
    Settings,
    adapter_config,
    default_model,
    load_settings,
    model_credential,
)


async def test_launch_existing_session_file_fails_before_remote_creation(monkeypatch, tmp_path):
    monkeypatch.setenv('ADAPTER_API_KEY', 'test-only-key')
    destination = tmp_path / 'session.json'
    destination.write_text('existing owner data')
    args = SimpleNamespace(command='launch', server='http://127.0.0.1:1',
                           meeting='https://meet.google.com/test', session_file=str(destination))
    with pytest.raises(FileExistsError):
        await manage(args)
    assert destination.read_text() == 'existing owner data'


async def test_switch_cli_sends_only_runtime_configuration(monkeypatch, capsys):
    import httpx

    from meetstream_agent_adapter import cli

    requests = []
    def respond(request):
        requests.append(request)
        return httpx.Response(200, json={"profile": "langgraph", "switched": True})
    client = httpx.AsyncClient
    monkeypatch.setenv("ADAPTER_API_KEY", "fixture-key")
    monkeypatch.setattr(cli.httpx, "AsyncClient", lambda **kwargs: client(transport=httpx.MockTransport(respond), **kwargs))
    await manage(SimpleNamespace(command="switch", server="http://127.0.0.1:8080", session="same-session",
                                 profile="langgraph", model=None, provider="google"))
    assert requests[0].url.path == "/v1/sessions/same-session/runtime"
    assert json.loads(requests[0].content) == {"profile": "langgraph", "provider": "google"}
    assert "fixture-key" not in capsys.readouterr().out


def test_private_reasoning_doctor_requires_no_meeting_credentials(monkeypatch, capsys):
    monkeypatch.setenv("ADAPTER_API_KEY", "a" * 40)
    monkeypatch.delenv("MEETSTREAM_API_KEY", raising=False)
    assert doctor(Settings(profile="fake", model="offline", output="events", transcript_source="deepgram")) == 0
    report = json.loads(capsys.readouterr().out)
    assert "MEETSTREAM_API_KEY" not in report["credential_present"]
    assert "public_https_origin" not in report["checks"]
    assert "DEEPGRAM_API_KEY" not in report["credential_present"]


def test_explicit_missing_environment_file_fails_before_inheriting_credentials(monkeypatch, tmp_path):
    monkeypatch.setenv("ADAPTER_API_KEY", "a" * 40)
    with pytest.raises(ValueError):
        load_environment(tmp_path / "missing.env")


def test_missing_config_does_not_select_a_default_runtime(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_settings(tmp_path / "missing.toml")


def test_langgraph_google_configuration_selects_native_model_and_google_credentials(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("AGENT_RUNTIME", "langgraph")
    monkeypatch.setenv("MODEL_PROVIDER", "google")
    monkeypatch.delenv("AGENT_MODEL", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("GOOGLE_API_KEY", "fixture-key-not-a-credential")
    monkeypatch.setenv("ADAPTER_API_KEY", "a" * 40)
    config = tmp_path / "config.toml"
    config.write_text('profile="openai"\nmodel="gpt-4.1-mini"\noutput="events"\n')
    settings = load_settings(config)
    assert settings.profile == "langgraph" and settings.model_provider == "google"
    assert settings.model == "gemini-3.5-flash-lite"
    doctor(settings)
    credentials = json.loads(capsys.readouterr().out)["credential_present"]
    assert credentials["GOOGLE_API_KEY"] and "OPENAI_API_KEY" not in credentials


@pytest.mark.parametrize("provider,model,key", [
    ("google", "gemini/gemini-3.5-flash-lite", "GOOGLE_API_KEY"),
    ("openai", "openai/gpt-4.1-mini", "OPENAI_API_KEY"),
])
def test_crewai_environment_selection_uses_configured_provider(monkeypatch, tmp_path, provider, model, key):
    monkeypatch.setenv("AGENT_RUNTIME", "crewai")
    monkeypatch.setenv("MODEL_PROVIDER", provider)
    monkeypatch.delenv("AGENT_MODEL", raising=False)
    config = tmp_path / "config.toml"
    config.write_text('profile="adk"\nmodel="gemini-3.5-flash-lite"\noutput="events"\n')
    settings = load_settings(config)
    assert settings.model == model and model_credential(settings) == key
    monkeypatch.setenv("AGENT_MODEL", "gemini/custom-model")
    assert load_settings(config).model == "gemini/custom-model"


@pytest.mark.parametrize("profile,provider,model,key", [
    ("openai", "openai", "gpt-4.1-mini", "CUSTOM_KEY"),
    ("langgraph", "openai", "gpt-4.1-mini", "CUSTOM_KEY"),
    ("langgraph", "google", "gemini-3.5-flash-lite", "GOOGLE_API_KEY"),
    ("adk", "google", "gemini-3.5-flash-lite", "GOOGLE_API_KEY"),
    ("crewai", "google", "gemini/gemini-3.5-flash-lite", "GOOGLE_API_KEY"),
    ("crewai", "openai", "openai/gpt-4.1-mini", "OPENAI_API_KEY"),
])
def test_shared_runtime_configuration_keeps_provider_and_credential_selection(profile, provider, model, key):
    assert default_model(profile, provider) == model
    settings = Settings(profile=profile, model=model, model_provider=provider, model_key_env="CUSTOM_KEY",
                        instructions="Owner instructions")
    assert model_credential(settings) == key
    config = adapter_config(settings, instructions="Demo instructions")
    assert config["model"] == model and config["instructions"] == "Owner instructions"
    if profile == "langgraph":
        assert config["provider"] == provider
    assert ("base_url" in config) == (profile in {"openai", "langgraph"})
