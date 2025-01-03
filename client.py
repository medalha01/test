# client.py

import socket
import threading
import time
import uuid
from utils import send_message, receive_message, get_server_port, serialize, deserialize, READ_REQUEST, COMMIT_REQUEST, COMMIT_RESPONSE
from broadcast import Broadcast

class Transaction:
    """Represents a single transaction."""
    def __init__(self, client_id, operations, broadcast, servers):
        """
        Initialize the transaction.
        
        Args:
            client_id (int): Identifier for the client.
            operations (list): List of operations, each being a dict with 'type', 'item', and 'value' (for writes).
            broadcast (Broadcast): Broadcast instance for commit requests.
            servers (list): List of (host, port) tuples for server replicas.
        """
        self.id = str(uuid.uuid4())
        self.client_id = client_id
        self.operations = operations
        self.broadcast = broadcast
        self.servers = servers
        self.ws = []  # Write set
        self.rs = []  # Read set
        self.selected_server = None  # Server selected for this transaction

    def execute(self):
        """Execute the transaction."""
        # Phase 1: Execution
        # Randomly select a server for this transaction
        import random
        self.selected_server = random.choice(self.servers)
        print(f"Transaction {self.id} selected server {self.selected_server}")
        for op in self.operations:
            if op['type'] == 'READ':
                self.read(op['item'])
            elif op['type'] == 'WRITE':
                self.write(op['item'], op['value'])
        # Phase 2: Termination
        # Send commit request via atomic broadcast
        commit_request = {
            'type': COMMIT_REQUEST,
            'transaction': {
                'id': self.id,
                'read_set': self.rs,
                'write_set': self.ws
            }
        }
        self.broadcast.broadcast(commit_request)
        # Wait for commit response
        # For simplicity, assume immediate response
        # In a real system, synchronization mechanisms are needed
        time.sleep(1)  # Wait for commit to propagate
        print(f"Transaction {self.id} execution completed.")

    def read(self, item):
        """Perform a read operation."""
        # Check if item is in write set
        for write_item in self.ws:
            if write_item['item'] == item:
                value = write_item['value']
                print(f"Transaction {self.id} read '{item}' from write set: {value}")
                self.rs.append({'item': item, 'value': value, 'version': None})  # Version unknown
                return
        # Otherwise, request from server
        read_request = {
            'type': READ_REQUEST,
            'item': item
        }
        try:
            send_message(self.selected_server[0], self.selected_server[1], read_request)
            # Receive response
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('', 0))  # Bind to any available port
                s.listen()
                conn, addr = self.selected_server
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_sock:
                    client_sock.connect((self.selected_server[0], self.selected_server[1]))
                    client_sock.sendall(serialize(read_request).encode())
                    response_data = client_sock.recv(4096)
                    response = deserialize(response_data.decode())
            if response['type'] == 'READ_RESPONSE':
                value = response['value']
                version = response['version']
                print(f"Transaction {self.id} read '{item}' from server: {value}, version: {version}")
                self.rs.append({'item': item, 'value': value, 'version': version})
        except Exception as e:
            print(f"Transaction {self.id} failed to read '{item}': {e}")

    def write(self, item, value):
        """Perform a write operation."""
        self.ws.append({'item': item, 'value': value})
        print(f"Transaction {self.id} wrote '{item}' = {value} locally.")

