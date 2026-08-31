"""Fetch broker asset and symbol metadata from cTrader Open API.

Saves a JSON file with assets, asset classes, symbol categories, account deposit
currency, and one joined record per tradable symbol (ids, names, swaps, etc.).

cTrader Open API does not provide a country field for instruments. Country-like
context is limited to symbol description text when the broker fills it in.

Requires application credentials, an access token, and a trading account ID from
Azure Key Vault. The default secret names are documented in README.md.

Note: Output will be saved to ABFSS path in Fabric lakehouse.
"""

import argparse
import csv
import json
import os
from pathlib import Path

from google.protobuf.json_format import MessageToDict
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks

from ctrader_open_api import Client, EndPoints, Protobuf, TcpProtocol
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAAccountAuthReq,
    ProtoOAApplicationAuthReq,
    ProtoOAAssetClassListReq,
    ProtoOAAssetListReq,
    ProtoOAErrorRes,
    ProtoOASymbolByIdReq,
    ProtoOASymbolCategoryListReq,
    ProtoOASymbolsListReq,
    ProtoOATraderReq,
)


SYMBOL_BY_ID_BATCH_SIZE = 100
REQUEST_TIMEOUT_SECONDS = 60
BROKER = "icmarkets"
DEFAULT_KEY_VAULT_URL = "https://ctrader.vault.azure.net/"

FLAT_CSV_COLUMNS = [
    "symbolId",
    "symbolName",
    "enabled",
    "description",
    "assetClassId",
    "assetClass",
    "symbolCategoryId",
    "symbolCategory",
    "baseAssetId",
    "baseAsset",
    "baseAssetDisplayName",
    "quoteAssetId",
    "quoteAsset",
    "quoteAssetDisplayName",
    "digits",
    "pipPosition",
    "lotSize",
    "tradingMode",
    "enableShortSelling",
    "swapLong",
    "swapShort",
    "swapRollover3Days",
    "swapCalculationType",
    "swapPeriod",
    "swapTime",
    "skipSWAPPeriods",
    "chargeSwapAtWeekends",
    "minVolume",
    "maxVolume",
    "stepVolume",
    "maxExposure",
    "commissionType",
    "preciseTradingCommissionRate",
    "scheduleTimeZone",
]


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client-id", default=None)
    parser.add_argument("--client-secret", default=None)
    parser.add_argument(
        "--access-token",
        default=None,
        help="Override the access token loaded from Key Vault.",
    )
    parser.add_argument(
        "--account-id",
        type=int,
        default=None,
        help="Override the cTrader account ID loaded from Azure Key Vault.",
    )
    parser.add_argument(
        "--host",
        choices=("live", "demo"),
        default=os.getenv("CTRADER_HOST", "live"),
        help="API host. Defaults to live.",
    )
    parser.add_argument(
        "--include-archived",
        action="store_true",
        help="Also request archived symbols.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("Files/ctrader_output/asset_metadata.json"),
        help="JSON file for the complete metadata dump.",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        help="Optional CSV of joined symbol rows.",
    )
    return parser.parse_args()


def get_secret(name):
    """Get a secret from Azure Key Vault using notebookutils."""
    try:
        import notebookutils
        return notebookutils.credentials.getSecret(DEFAULT_KEY_VAULT_URL, name)
    except ImportError:
        # Fallback for environments without notebookutils
        # In practice, this should be handled by the execution environment
        raise SystemExit("notebookutils is required for secret retrieval")


def load_credentials():
    """Load all required credentials from Azure Key Vault."""
    return {
        "clientId": get_secret(os.getenv("CTRADER_CLIENT_ID_SECRET", "ctrader-app-client-id")),
        "clientSecret": get_secret(os.getenv("CTRADER_CLIENT_SECRET_SECRET", "ctrader-app-client-secret")),
        "accessToken": get_secret(os.getenv("CTRADER_ACCESS_TOKEN_SECRET", f"ctrader-access-token-{BROKER}")),
        "accountId": int(get_secret(os.getenv("CTRADER_ACCOUNT_ID_SECRET", f"ctrader-account-id-{BROKER}"))),
    }


def resolve_account_and_host(args):
    credentials = load_credentials()
    account_id = credentials["accountId"]
    return account_id, args.host.lower()


def proto_to_dict(message):
    return MessageToDict(message, preserving_proto_field_name=True)


def as_id(value):
    if value is None or value == "":
        return None
    return int(value)


def extract_or_fail(message):
    payload = Protobuf.extract(message)
    if payload.payloadType == ProtoOAErrorRes().payloadType:
        error = payload
        raise RuntimeError(
            f"{error.errorCode}: {error.description}".strip(": ")
        )
    return payload


def send(client, request):
    deferred = client.send(request, responseTimeoutInSeconds=REQUEST_TIMEOUT_SECONDS)
    deferred.addCallback(extract_or_fail)
    return deferred


def index_by(items, key):
    return {as_id(item[key]): item for item in items if key in item}


def join_symbol(light, details, assets_by_id, classes_by_id, categories_by_id):
    category = categories_by_id.get(as_id(light.get("symbolCategoryId")), {})
    asset_class = classes_by_id.get(as_id(category.get("assetClassId")), {})
    base_asset = assets_by_id.get(as_id(light.get("baseAssetId")), {})
    quote_asset = assets_by_id.get(as_id(light.get("quoteAssetId")), {})

    joined = dict(details)
    joined.update(light)
    joined["assetClassId"] = category.get("assetClassId")
    joined["assetClass"] = asset_class.get("name")
    joined["symbolCategory"] = category.get("name")
    joined["baseAsset"] = base_asset.get("name")
    joined["baseAssetDisplayName"] = base_asset.get("displayName")
    joined["quoteAsset"] = quote_asset.get("name")
    joined["quoteAssetDisplayName"] = quote_asset.get("displayName")
    return joined


def write_csv(path, rows):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FLAT_CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in FLAT_CSV_COLUMNS})


def main():
    args = parse_args()
    
    # Load credentials using notebookutils
    credentials = load_credentials()
    args.client_id = credentials["clientId"]
    args.client_secret = credentials["clientSecret"]
    
    if not args.client_id or not args.client_secret:
        raise SystemExit(
            "Provide --client-id and --client-secret, or set CTRADER_CLIENT_ID "
            "and CTRADER_CLIENT_SECRET."
        )

    access_token = credentials["accessToken"]
    account_id, host = resolve_account_and_host(args)
    api_host = (
        EndPoints.PROTOBUF_LIVE_HOST if host == "live" else EndPoints.PROTOBUF_DEMO_HOST
    )
    client = Client(api_host, EndPoints.PROTOBUF_PORT, TcpProtocol)
    outcome = {"error": None}

    @inlineCallbacks
    def run(_connected_client):
        try:
            app_auth = ProtoOAApplicationAuthReq()
            app_auth.clientId = args.client_id
            app_auth.clientSecret = args.client_secret
            yield send(client, app_auth)
            print("Application authorized")

            account_auth = ProtoOAAccountAuthReq()
            account_auth.ctidTraderAccountId = account_id
            account_auth.accessToken = access_token
            yield send(client, account_auth)
            print(f"Account {account_id} authorized")

            asset_req = ProtoOAAssetListReq()
            asset_req.ctidTraderAccountId = account_id
            assets = [proto_to_dict(item) for item in (yield send(client, asset_req)).asset]
            print(f"Loaded {len(assets)} assets")

            class_req = ProtoOAAssetClassListReq()
            class_req.ctidTraderAccountId = account_id
            asset_classes = [
                proto_to_dict(item)
                for item in (yield send(client, class_req)).assetClass
            ]
            print(f"Loaded {len(asset_classes)} asset classes")

            category_req = ProtoOASymbolCategoryListReq()
            category_req.ctidTraderAccountId = account_id
            categories = [
                proto_to_dict(item)
                for item in (yield send(client, category_req)).symbolCategory
            ]
            print(f"Loaded {len(categories)} symbol categories")

            trader_req = ProtoOATraderReq()
            trader_req.ctidTraderAccountId = account_id
            trader = proto_to_dict((yield send(client, trader_req)).trader)

            symbols_req = ProtoOASymbolsListReq()
            symbols_req.ctidTraderAccountId = account_id
            symbols_req.includeArchivedSymbols = args.include_archived
            symbols_res = yield send(client, symbols_req)
            light_symbols = [proto_to_dict(item) for item in symbols_res.symbol]
            archived_symbols = [proto_to_dict(item) for item in symbols_res.archivedSymbol]
            print(f"Loaded {len(light_symbols)} symbols")

            details_by_id = {}
            symbol_details = []
            symbol_ids = [as_id(item["symbolId"]) for item in light_symbols]
            for start in range(0, len(symbol_ids), SYMBOL_BY_ID_BATCH_SIZE):
                batch = symbol_ids[start : start + SYMBOL_BY_ID_BATCH_SIZE]
                details_req = ProtoOASymbolByIdReq()
                details_req.ctidTraderAccountId = account_id
                details_req.symbolId.extend(batch)
                details_res = yield send(client, details_req)
                for item in details_res.symbol:
                    details = proto_to_dict(item)
                    symbol_details.append(details)
                    details_by_id[as_id(details["symbolId"])] = details
                print(
                    f"Loaded full symbol details "
                    f"{min(start + SYMBOL_BY_ID_BATCH_SIZE, len(symbol_ids))}/{len(symbol_ids)}"
                )

            assets_by_id = index_by(assets, "assetId")
            classes_by_id = index_by(asset_classes, "id")
            categories_by_id = index_by(categories, "id")
            deposit_asset = assets_by_id.get(as_id(trader.get("depositAssetId")), {})

            joined_symbols = [
                join_symbol(
                    light,
                    details_by_id.get(as_id(light["symbolId"]), {}),
                    assets_by_id,
                    classes_by_id,
                    categories_by_id,
                )
                for light in light_symbols
            ]

            output = {
                "ctidTraderAccountId": account_id,
                "host": host,
                "depositAssetId": trader.get("depositAssetId"),
                "depositCurrency": deposit_asset.get("name"),
                "brokerName": trader.get("brokerName"),
                "assets": assets,
                "assetClasses": asset_classes,
                "symbolCategories": categories,
                "trader": trader,
                "archivedSymbols": archived_symbols,
                "lightSymbols": light_symbols,
                "symbolDetails": symbol_details,
                "symbols": joined_symbols,
            }
            
            # Ensure the output path supports ABFSS format
            output_path = args.output
            try:
                # Try to write to the ABFSS path
                output_path.write_text(
                    json.dumps(output, indent=2, default=str) + "\n",
                    encoding="utf-8",
                )
                print(f"Saved metadata for {len(joined_symbols)} symbols to {output_path}")
            except Exception as e:
                print(f"Failed to write to ABFSS path {output_path}: {e}")
                # Fallback to local file if ABFSS fails
                fallback_path = Path("asset_metadata.json")
                fallback_path.write_text(
                    json.dumps(output, indent=2, default=str) + "\n",
                    encoding="utf-8",
                )
                print(f"Fallback: Saved metadata to {fallback_path}")
                output_path = fallback_path

            if args.csv:
                # Handle CSV output similarly
                try:
                    write_csv(args.csv, joined_symbols)
                    print(f"Saved symbol CSV to {args.csv}")
                except Exception as e:
                    print(f"Failed to write CSV to {args.csv}: {e}")
                    # Fallback to local CSV
                    fallback_csv = Path("asset_metadata.csv")
                    write_csv(fallback_csv, joined_symbols)
                    print(f"Fallback: Saved symbol CSV to {fallback_csv}")
        except Exception as error:  # noqa: BLE001 - surface API/network failures, then stop reactor
            outcome["error"] = error
            print(f"Failed to fetch metadata: {error}")
        finally:
            client.stopService()
            if reactor.running:
                reactor.stop()

    def on_disconnect(_client, reason):
        print(f"Disconnected: {reason.value}")

    client.setConnectedCallback(run)
    client.setDisconnectedCallback(on_disconnect)
    client.startService()
    reactor.run()

    if outcome["error"] is not None:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
