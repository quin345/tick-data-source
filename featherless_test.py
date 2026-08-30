#!/usr/bin/env python3
import os
from pathlib import Path

import requests
from dotenv import load_dotenv


def main() -> None:
    load_dotenv(Path(__file__).resolve().parent / ".env")

    api_key = os.getenv("FEATHERLESS_API_KEY")
    base_url = os.getenv("OPENAI_API_BASE", "https://api.featherless.ai/v1")
    model = os.getenv("FEATHERLESS_MODEL", "Qwen/Qwen2.5-1.5B-Instruct")

    if not api_key:
        raise SystemExit(
            "FEATHERLESS_API_KEY is not set. Add your key to .env or copy .env.example to .env first."
        )

    response = requests.post(
        f"{base_url}/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "messages": [{"role": "user", "content": "Say hello from Featherless AI in one sentence."}],
            "temperature": 0.2,
            "max_tokens": 100,
        },
        timeout=90,
    )
    response.raise_for_status()
    payload = response.json()
    print(payload)


if __name__ == "__main__":
    main()
