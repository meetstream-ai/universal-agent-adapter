"""Lazy imports keep the core usable without any framework installed."""

import importlib

MODULES = {"openai": "openai", "langgraph": "langgraph", "adk": "adk", "crewai": "crewai", "fake": "fake"}


def load_adapter(name: str, **config):
    if name not in MODULES:
        raise ValueError("Unknown runtime profile")
    module = importlib.import_module(f"{__name__}.{MODULES[name]}")
    return module.Adapter(**config)
