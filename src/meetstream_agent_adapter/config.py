import os
import tomllib
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from pydantic import Field, field_validator, model_validator

from .contract import StrictModel

RUNTIME_PACKAGES = {"openai": "openai-agents", "langgraph": "langgraph", "adk": "google-adk", "crewai": "crewai"}
DEFAULT_MODELS = {"openai": "gpt-4.1-mini", "langgraph": "gpt-4.1-mini", "adk": "gemini-3.5-flash-lite",
                  "crewai": "openai/gpt-4.1-mini", "fake": "offline"}


def validate_meeting_link(value):
    try:
        url = urlparse(value)
        host = url.hostname or ""
        valid = (len(value) <= 2048 and not any(ord(char) < 33 or ord(char) == 127 for char in value)
                 and url.scheme == "https" and not url.username and not url.password
                 and url.port in {None, 443} and url.path not in {"", "/"}
                 and (host in {"meet.google.com", "teams.microsoft.com", "teams.live.com", "zoom.us"}
                      or host.endswith(".zoom.us")))
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("Supply a valid HTTPS Google Meet, Zoom or Microsoft Teams meeting link")
    return value


def default_model(profile, provider="openai"):
    if provider == "google" and profile in {"langgraph", "crewai"}:
        return ("gemini/" if profile == "crewai" else "") + DEFAULT_MODELS["adk"]
    return DEFAULT_MODELS[profile]


class RuntimeSelection(StrictModel):
    profile: Literal["openai", "langgraph", "adk", "crewai"]
    model: str | None = Field(default=None, min_length=1, max_length=200)
    provider: Literal["openai", "google"] | None = None

    def settings(self, base):
        provider = self.provider or base.model_provider
        return Settings.model_validate({**base.model_dump(), "profile": self.profile,
                                        "model": self.model or default_model(self.profile, provider),
                                        "model_provider": provider})


class Settings(StrictModel):
    profile: Literal["openai", "langgraph", "adk", "crewai", "fake"] = "openai"
    model: str = "gpt-4.1-mini"
    workspace: str = "default"
    output: Literal["chat", "voice_and_chat", "events"] = "voice_and_chat"
    transcript_source: Literal["meetstream", "deepgram"] = "meetstream"
    wake_words: list[str] = Field(default_factory=lambda: ["hey assistant", "hey bot"])
    followup_seconds: int = Field(default=30, ge=0, le=300)
    turn_timeout: float = Field(default=60, ge=1, le=300)
    max_sessions: int = Field(default=8, ge=1, le=100)
    session_ttl: int = Field(default=3600, ge=60, le=86400)
    reconnect_grace: int = Field(default=15, ge=1, le=60)
    waiting_room_timeout: int = Field(default=180, ge=60, le=600)
    public_url: str = ""
    ledger: str = "actions.sqlite"
    speech_model: str = "tts-1"
    speech_voice: str = "alloy"
    speech_base_url: str = "https://api.openai.com/v1"
    speech_provider: Literal["openai", "gemini", "deepgram"] = "openai"
    model_base_url: str | None = None
    model_provider: Literal["openai", "google"] = "openai"
    model_key_env: str = "OPENAI_API_KEY"
    model_api: Literal["responses", "chat_completions"] = "responses"
    instructions: str | None = Field(default=None, min_length=1, max_length=16000)
    voice_switching: bool = False
    mia_config_id: str | None = None

    @model_validator(mode="after")
    def one_reasoning_owner(self):
        if self.profile == "adk" and self.model.startswith("gemini/"):
            raise ValueError("ADK uses a Gemini model name without CrewAI's gemini/ provider prefix")
        if self.mia_config_id:
            raise ValueError("Hosted MIA configurations cannot be combined with the public media bridge; use the MIA custom-function bridge")
        return self

    @field_validator("public_url")
    @classmethod
    def validate_public_url(cls, value):
        parsed = urlparse(value)
        if value and (parsed.scheme != "https" or not parsed.hostname or parsed.query or parsed.fragment
                      or parsed.username or parsed.path not in {"", "/"}):
            raise ValueError("public_url must be an HTTPS origin without a query")
        return value.rstrip("/")


def load_settings(path="config.toml") -> Settings:
    # A mistyped filename must not silently start a different default runtime.
    data = tomllib.loads(Path(path).read_text())
    if os.getenv("MODEL_PROVIDER"):
        data["model_provider"] = os.environ["MODEL_PROVIDER"]
    if os.getenv("AGENT_RUNTIME"):
        data["profile"] = os.environ["AGENT_RUNTIME"]
        # Models stay explicit in the profile; CLI allows an explicit override.
        if data["profile"] not in DEFAULT_MODELS:
            raise ValueError("Unknown AGENT_RUNTIME")
        provider = data.get("model_provider", "openai")
        data["model"] = os.getenv("AGENT_MODEL", default_model(data["profile"], provider))
    elif os.getenv("AGENT_MODEL"):
        data["model"] = os.environ["AGENT_MODEL"]
    if os.getenv("PUBLIC_BASE_URL"):
        data["public_url"] = os.environ["PUBLIC_BASE_URL"]
    for name in ["SPEECH_PROVIDER", "SPEECH_MODEL", "SPEECH_VOICE", "MODEL_BASE_URL", "MODEL_KEY_ENV", "MODEL_API", "TRANSCRIPT_SOURCE"]:
        if os.getenv(name):
            data[name.lower()] = os.environ[name]
    return Settings.model_validate(data)


def runtime_provider(settings):
    if settings.profile == "fake":
        return "offline"
    if (settings.profile == "adk" or settings.profile == "langgraph" and settings.model_provider == "google"
            or settings.profile == "crewai" and settings.model.startswith("gemini/")):
        return "google"
    return "openai"


def model_credential(settings):
    provider = runtime_provider(settings)
    if provider == "offline":
        return None
    if provider == "google":
        return "GOOGLE_API_KEY"
    return settings.model_key_env if settings.profile in {"openai", "langgraph"} else "OPENAI_API_KEY"


def adapter_config(settings, *, instructions=None):
    config = {"model": settings.model}
    if settings.instructions or instructions:
        config["instructions"] = settings.instructions or instructions
    if settings.profile in {"openai", "langgraph"}:
        config.update(base_url=settings.model_base_url, key_env=settings.model_key_env)
    if settings.profile == "openai":
        config["model_api"] = settings.model_api
    elif settings.profile == "langgraph":
        config["provider"] = settings.model_provider
    return config
