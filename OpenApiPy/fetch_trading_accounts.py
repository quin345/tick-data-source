"""Fetch cTrader trading-account information and save the complete response."""

import argparse
import json
import os
from pathlib import Path

import requests


TRADING_ACCOUNTS_URL = "https://api.spotware.com/connect/tradingaccounts"
BROKER = "icmarkets"
DEFAULT_KEY_VAULT_URL = "https://ctrader.vault.azure.net/"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("account_info.json"),
        help="JSON file for the complete API response.",
    )
    return parser.parse_args()


def load_access_token():
    vault_url = os.getenv("AZURE_KEY_VAULT_URL", DEFAULT_KEY_VAULT_URL)

    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except ImportError as error:
        raise SystemExit(
            "Azure Key Vault support requires azure-identity and "
            "azure-keyvault-secrets to be installed."
        ) from error

    secret_client = SecretClient(
        vault_url=vault_url,
        credential=DefaultAzureCredential(),
    )
    secret_name = os.getenv(
        "CTRADER_ACCESS_TOKEN_SECRET", f"ctrader-access-token-{BROKER}"
    )
    access_token = secret_client.get_secret(secret_name).value
    if not access_token:
        raise SystemExit(f"Azure Key Vault secret '{secret_name}' is empty.")
    return access_token


def main():
    args = parse_args()
    access_token = load_access_token()
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