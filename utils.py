# utils.py

import json
import threading

# Configuration Parameters
SEQUENCER_HOST = 'localhost'
SEQUENCER_PORT = 10000

SERVER_HOST = 'localhost'
SERVER_BASE_PORT = 10001  # Servers will listen on SERVER_BASE_PORT + server_id

CLIENT_HOST = 'localhost'
CLIENT_BASE_PORT = 11000  # Clients will use ports starting from CLIENT_BASE_PORT

# Message Types
READ_REQUEST = 'READ_REQUEST'
READ_RESPONSE = 'READ_RESPONSE'
COMMIT_REQUEST = 'COMMIT_REQUEST'
COMMIT_RESPONSE = 'COMMIT_RESPONSE'
BROADCAST_MESSAGE = 'BROADCAST_MESSAGE'
SEQUENCER_REQUEST = 'SEQUENCER_REQUEST'
SEQUENCER_RESPONSE = 'SEQUENCER_RESPONSE'

def serialize(message):
    """Serialize a message dictionary to JSON string."""
    return json.dumps(message)

def deserialize(message_str):
    """Deserialize a JSON string to a message dictionary."""
    return json.loads(message_str)

def get_server_port(server_id):
    """Get the port number for a given server ID."""
    return SERVER_BASE_PORT + server_id

def get_client_port(client_id):
    """Get the port number for a given client ID."""
    return CLIENT_BASE_PORT + client_id

def send_message(host, port, message):
    """Send a message to the specified host and port."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(serialize(message).encode())

def receive_message(conn):
    """Receive a message from a socket connection."""
    import socket
    data = conn.recv(4096)
    if not data:
        return None
    return deserialize(data.decode())
   


def get_server_port(server_id):
    """Return the port number for a given server ID."""
    return SERVER_BASE_PORT + server_id



