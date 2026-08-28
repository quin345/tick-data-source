"""Authorize a cTrader Open API application and print its OAuth token."""

import argparse
import http.server
import json
import os
from pathlib import Path
import threading
import urllib.parse
import webbrowser

import requests


AUTHORIZATION_URL = "https://id.ctrader.com/my/settings/openapi/grantingaccess/"
TOKEN_URL = "https://openapi.ctrader.com/apps/token"
DEFAULT_KEY_VAULT_URL = "https://ctrader.vault.azure.net/"


class OAuthCallbackHandler(http.server.BaseHTTPRequestHandler):
    """Capture one OAuth callback and return a small browser response."""

    result = None

    def do_GET(self):  # noqa: N802 - required by BaseHTTPRequestHandler
        request = urllib.parse.urlparse(self.path)
        if request.path != self.server.callback_path:
            self.send_error(404)
            return

        OAuthCallbackHandler.result = urllib.parse.parse_qs(request.query)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(
            b"<html><body><h1>Authorization received</h1>"
            b"<p>You can close this window.</p></body></html>"
        )
        threading.Thread(target=self.server.shutdown, daemon=True).start()

    def log_message(self, *_args):
        return


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--client-id",
        default=None,
        help="Override the client ID from Azure Key Vault or CTRADER_CLIENT_ID.",
    )
    parser.add_argument(
        "--client-secret",
        default=None,
        help="Override the client secret from Azure Key Vault or CTRADER_CLIENT_SECRET.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("auth_tokens.json"),
        help="JSON file to write after authorization succeeds.",
    )
    parser.add_argument(
        "--redirect-uri",
        default=os.getenv("CTRADER_REDIRECT_URI", "http://127.0.0.1:8765/callback"),
        help="Must exactly match a redirect URI registered for the application.",
    )
    parser.add_argument("--scope", choices=("trading", "accounts"), default="trading")
    parser.add_argument("--no-browser", action="store_true")
    return parser.parse_args()


def load_client_credentials(args):
    """Load OAuth client credentials, preferring Key Vault when configured."""
    if args.client_id and args.client_secret:
        return args.client_id, args.client_secret

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
    client_id_secret = os.getenv("CTRADER_CLIENT_ID_SECRET", "ctrader-app-client-id")
    client_secret_secret = os.getenv(
        "CTRADER_CLIENT_SECRET_SECRET", "ctrader-app-client-secret"
    )
    client_id = secret_client.get_secret(client_id_secret).value
    client_secret = secret_client.get_secret(client_secret_secret).value
    return client_id, client_secret

    return args.client_id or os.getenv("CTRADER_CLIENT_ID"), args.client_secret or os.getenv(
        "CTRADER_CLIENT_SECRET"
    )


def main():
    args = parse_args()
    args.client_id, args.client_secret = load_client_credentials(args)
    if not args.client_id or not args.client_secret:
        raise SystemExit(
            "Provide --client-id and --client-secret, set AZURE_KEY_VAULT_URL, "
            "or set CTRADER_CLIENT_ID and CTRADER_CLIENT_SECRET."
        )

    redirect = urllib.parse.urlparse(args.redirect_uri)
    if redirect.scheme != "http" or redirect.hostname not in {"127.0.0.1", "localhost"}:
        raise SystemExit("For this script, --redirect-uri must be an http localhost URL.")

    authorization_query = urllib.parse.urlencode(
        {
            "client_id": args.client_id,
            "redirect_uri": args.redirect_uri,
            "scope": args.scope,
            "product": "web",
        }
    )
    authorization_url = f"{AUTHORIZATION_URL}?{authorization_query}"

    server = http.server.HTTPServer(
        (redirect.hostname, redirect.port or 80), OAuthCallbackHandler
    )
    server.callback_path = redirect.path or "/"

    print(f"Opening {authorization_url}")
    if not args.no_browser:
        webbrowser.open(authorization_url)
    else:
        print("Open the URL above in a browser.")

    OAuthCallbackHandler.result = None
    server.serve_forever()
    callback = OAuthCallbackHandler.result or {}
    if "error" in callback:
        description = callback.get("description", callback["error"])[0]
        raise SystemExit(f"Authorization failed: {description}")
    if "code" not in callback:
        raise SystemExit("Authorization callback did not contain a code.")

    response = requests.get(
        TOKEN_URL,
        params={
            "grant_type": "authorization_code",
            "code": callback["code"][0],
            "redirect_uri": args.redirect_uri,
            "client_id": args.client_id,
            "client_secret": args.client_secret,
        },
        timeout=30,
    )
    response.raise_for_status()
    token = response.json()
    if token.get("errorCode") or token.get("error"):
        raise SystemExit(json.dumps(token, indent=2))

    token_json = json.dumps(token, indent=2) + "\n"
    args.output.write_text(token_json, encoding="utf-8")
    print(token_json, end="")
    print(f"Saved token data to {args.output}")


if __name__ == "__main__":
    main()