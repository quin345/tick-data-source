#!/usr/bin/env python3
import os
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI


def main() -> None:
    load_dotenv(Path(__file__).resolve().parent / ".env")

    api_key = os.getenv("FEATHERLESS_API_KEY")
    base_url = os.getenv("OPENAI_API_BASE", "https://api.featherless.ai/v1")
    model = os.getenv("FEATHERLESS_MODEL", "Qwen/Qwen3.8-27B")

    if not api_key:
        raise SystemExit(
            "FEATHERLESS_API_KEY is not set. Add your key to .env or copy .env.example to .env first."
        )

    client = OpenAI(api_key=api_key, base_url=base_url)

    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": "Say hello from Featherless AI in one sentence."}],
        temperature=0.7,
    )

    print(response.choices[0].message.content)


if __name__ == "__main__":
    main()
