# workflow_engine/utils/env.py
import os

from dotenv import load_dotenv

load_dotenv()


def get_env(key: str, default: str | None = None) -> str:
    value = os.getenv(key)
    if not value:
        if default is None:
            raise ValueError(f"Environment variable {key} is not set")
        return default
    return value


__all__ = [
    "get_env",
]
