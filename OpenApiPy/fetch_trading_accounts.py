"""Fetch cTrader trading-account information and save the complete response."""

import argparse
import json
import os
from pathlib import Path

import requests


TRADING_ACCOUNTS_URL = "https://api.spotware.com/connect/tradingaccounts"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--token-file",
        type=Path,
        default=Path("auth_tokens.json"),
        help="JSON file containing accessToken.",
    )
    parser.add_argument(
        "--access-token",
        default=os.getenv("CTRADER_ACCESS_TOKEN"),
        help="Use this token instead of reading the token file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("account_info.json"),
        help="JSON file for the complete API response.",
    )
    return parser.parse_args()


def load_access_token(token_file, access_token):
    if access_token:
        return access_token

    try:
        token_data = json.loads(token_file.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise SystemExit(f"Token file not found: {token_file}") from error
    except json.JSONDecodeError as error:
        raise SystemExit(f"Token file is not valid JSON: {token_file}") from error

    access_token = token_data.get("accessToken")
    if not access_token:
        raise SystemExit(f"No accessToken found in {token_file}")
    return access_token


def main():
    args = parse_args()
    access_token = load_access_token(args.token_file, args.access_token)
    response = requests.get(
        TRADING_ACCOUNTS_URL,
        params={"access_token": access_token},
        timeout=30,
    )
    response.raise_for_status()

    try:
        account_data = response.json()
    except requests.exceptions.JSONDecodeError as error:
        raise SystemExit("The trading-accounts response was not valid JSON.") from error

    output_json = json.dumps(account_data, indent=2) + "\n"
    args.output.write_text(output_json, encoding="utf-8")
    print(f"Saved account information to {args.output}")


if __name__ == "__main__":
    main()