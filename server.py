# server.py

import socket
import threading

# server.py

from utils import (
    receive_message,
    serialize,
    deserialize,
    get_server_port,
    get_paxos_port,
    READ_REQUEST,
    READ_RESPONSE,
    COMMIT_REQUEST,
    COMMIT_RESPONSE,
    BROADCAST_MESSAGE,
)
from broadcast import Broadcast
from paxos import PaxosProposer, PaxosAccepter, PaxosLearner


class Server:
    def __init__(self, server_id, replicas):
        self.server_id = server_id
        self.host = "localhost"
        self.port = get_server_port(server_id)
        self.data_store = {}  # key: (value, version)
        self.lock = threading.Lock()
        self.broadcast = Broadcast(
            replicas=[(self.host, get_server_port(r)) for r in replicas]
        )

        # Assign unique Paxos ports
        proposer_port = get_paxos_port(server_id)
        accepter_port = proposer_port + 1
        learner_port = proposer_port + 2

        # Initialize Paxos roles with peers excluding the current server
        self.peers = [
            (self.host, get_paxos_port(r)) for r in replicas if r != server_id
        ]
        self.proposer = PaxosProposer(server_id, self.host, proposer_port, self.peers)
        self.accepter = PaxosAccepter(server_id, self.host, accepter_port)
        self.learner = PaxosLearner(server_id, self.host, learner_port)

    def start(self):
        """Start the server."""
        threading.Thread(target=self.proposer.receive_message, daemon=True).start()
        threading.Thread(target=self.accepter.receive_message, daemon=True).start()
        threading.Thread(target=self.learner.receive_message, daemon=True).start()

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen()
        print(f"Server {self.server_id} listening on {self.host}:{self.port}")
        while True:
            conn, addr = server_socket.accept()
            threading.Thread(target=self.handle_connection, args=(conn, addr)).start()

    def handle_connection(self, conn, addr):
        """Handle incoming messages."""
        try:
            message = receive_message(conn)
            if not message:
                return
            msg_type = message.get("type")
            if msg_type == READ_REQUEST:
                self.handle_read_request(conn, message)
            elif msg_type == COMMIT_REQUEST:
                self.handle_commit_request(message)
            elif msg_type == BROADCAST_MESSAGE:
                self.handle_broadcast_message(message)
            else:
                print(f"Unknown message type: {msg_type}")
        except Exception as e:
            print(f"Error handling connection from {addr}: {e}")
        finally:
            conn.close()

    def handle_read_request(self, conn, message):
        """Handle read requests from clients."""
        item = message["item"]
        with self.lock:
            value, version = self.data_store.get(item, (None, 0))
        response = {
            "type": READ_RESPONSE,
            "item": item,
            "value": value,
            "version": version,
        }
        conn.sendall(serialize(response).encode())
        print(
            f"Server {self.server_id} handled READ_REQUEST for item '{item}': value={value}, version={version}"
        )

    def handle_commit_request(self, message):
        """Handle commit requests and start Paxos consensus."""
        transaction = message["transaction"]
        ws = transaction["write_set"]
        proposed_value = {"transaction_id": transaction["id"], "write_set": ws}

        # Start Paxos consensus to replicate the write operation
        print(
            f"Server {self.server_id} initiating Paxos for transaction {transaction['id']}."
        )
        self.proposer.propose(proposed_value)

    def handle_broadcast_message(self, message):
        """Handle broadcast messages."""
        msg = message.get("message")
        if not msg:
            print(f"Server {self.server_id} received invalid broadcast message.")
            return

        if msg["type"] == COMMIT_REQUEST:
            transaction = msg["transaction"]
            transaction_id = transaction["id"]
            write_set = transaction["write_set"]
            print(
                f"Server {self.server_id} processing COMMIT_REQUEST for transaction {transaction_id}"
            )
            self.apply_commit(transaction_id, write_set)
        else:
            print(
                f"Server {self.server_id} received unknown message type: {msg['type']}"
            )

    def apply_commit(self, transaction_id, write_set):
        """Apply the write set after consensus."""
        print(
            f"Server {self.server_id} applying commit for transaction {transaction_id} with write set: {write_set}"
        )
        with self.lock:
            for write in write_set:
                item, value = write["item"], write["value"]
                _, current_version = self.data_store.get(item, (None, 0))
                self.data_store[item] = (value, current_version + 1)
            print(f"Server {self.server_id} updated data store: {self.data_store}")

    def get_data_store(self):
        """Return the current state of the data store."""
        with self.lock:
            return dict(
                self.data_store
            )  # Return a copy of the data store to avoid race conditions
