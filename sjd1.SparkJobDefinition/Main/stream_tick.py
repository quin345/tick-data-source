from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

from client1 import Client
from protobuf1 import Protobuf
from tcpProtocol import TcpProtocol
from auth import Auth
from endpoints import EndPoints
import OpenApiMessages_pb2 as OA
import OpenApiModelMessages_pb2 as OAModel
import OpenApiCommonMessages_pb2 as OACommon
import OpenApiCommonModelMessages_pb2 as OAModelCommon
from twisted.internet import reactor
import json
from azure.eventhub import EventHubProducerClient, EventData
from datetime import datetime, timezone
import time

credentials = json.load(open(f"{notebookutils.nbResPath}/builtin/credentials.json"))

# Replace the placeholders with your Event Hubs connection string and event hub name
EVENTHUB_NAME = credentials['eventHubName']
CONNECTION_STR = credentials['connectionString']

# Create a producer client to send messages to the event hub
producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)



client = Client(EndPoints.PROTOBUF_LIVE_HOST, EndPoints.PROTOBUF_PORT, TcpProtocol)
PROTO_OA_ERROR_RES_PAYLOAD_TYPE = OA.ProtoOAErrorRes().payloadType

tickers = []
symbol_ids = {}

def safe_stop():
    if reactor.running:
        reactor.stop()

def reconnect():
    print("Reconnecting in 3 seconds...")
    reactor.callLater(3, client.startService)


def onAccAuth(message):
    if message.payloadType == PROTO_OA_ERROR_RES_PAYLOAD_TYPE:
        print('Account authentication failed:', Protobuf.extract(message))
        reactor.stop()
        return  
    req = OA.ProtoOASymbolsListReq()
    req.ctidTraderAccountId = credentials['accountId']
    print('Account authenticated')
    deferred = client.send(req)
    deferred.addCallbacks(onSymbolsList, onError)
    print('Requesting symbol list...')


def onSymbolsList(message):
    response = Protobuf.extract(message)
    print('Symbols received')
    tickers.clear()
    symbol_ids.clear()
    for symbol in response.symbol:
        tickers.append(symbol.symbolName)
        symbol_ids[symbol.symbolName] = symbol.symbolId
        print(f'{symbol.symbolName} -> SymbolID {symbol.symbolId}')
    subscribeToPrices()


def subscribeToPrices():
    if not symbol_ids:
        print("No symbols yet, retrying...")
        reactor.callLater(1, subscribeToPrices)
        return

    print("Subscribing to price streams...")
    req = OA.ProtoOASubscribeSpotsReq()
    req.ctidTraderAccountId = credentials['accountId']
    req.symbolId.extend(symbol_ids.values())
    client.send(req)

def get_current_timestamp():
    """Return the current timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()

def onMsg(client, message):
    if message.payloadType == OA.ProtoOASpotEvent().payloadType:
        response = Protobuf.extract(message)
        symbolName = next((n for n, sid in symbol_ids.items() if sid == response.symbolId), str(response.symbolId))
        # print(f'Price update: {symbolName} Bid {response.bid} Ask {response.ask}')
        
        payload = {
            "timestamp": get_current_timestamp(),
            "Symbol": symbolName,
            "Bid": response.bid,
            "Ask": response.ask
        }
        message1 = json.dumps(payload)
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(message1))
        producer.send_batch(event_data_batch)

        


def onAppAuth(message):
    if message.payloadType == PROTO_OA_ERROR_RES_PAYLOAD_TYPE:
        print('App authentication failed:', Protobuf.extract(message))
        reactor.stop()
        return
    print('App authenticated')
    req = OA.ProtoOAAccountAuthReq()
    req.ctidTraderAccountId = credentials['accountId']
    req.accessToken = credentials['accessToken']
    deferred = client.send(req)
    deferred.addCallbacks(onAccAuth, onError)


def onError(failure):
    print('Error:', repr(failure.value))
    reactor.stop()


def connected(client):
    print('Connected')
    req = OA.ProtoOAApplicationAuthReq()
    req.clientId = credentials['clientId']
    req.clientSecret = credentials['clientSecret']
    deferred = client.send(req, responseTimeoutInSeconds=20)
    deferred.addCallbacks(onAppAuth, onError)


def disconnected(client, reason):
    print("Disconnected:", reason)
    reconnect()


client.setConnectedCallback(connected)
client.setDisconnectedCallback(disconnected)
client.setMessageReceivedCallback(onMsg)
client.startService()
reactor.run()
print("producer closed")
producer.close()