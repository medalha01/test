# server.py

import socket
import threading
from utils import (
    receive_message,
    serialize,
    deserialize,
    get_server_port,
    READ_REQUEST,
    READ_RESPONSE,
    COMMIT_REQUEST,
    COMMIT_RESPONSE,
    BROADCAST_MESSAGE,
)
from broadcast import Broadcast


class Server:
    """Data Managing Server with replication."""

    def __init__(self, server_id, replicas):
        self.server_id = server_id
        self.host = "localhost"
        self.port = get_server_port(server_id)
        self.data_store = {}  # key: (value, version)
        self.lock = threading.Lock()
        self.broadcast = Broadcast(replicas=replicas)

    def start(self):
        """Start the server."""
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
                self.handle_commit_request(conn, message)
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

    def handle_commit_request(self, conn, message):
        """Handle commit requests and perform certification test."""
        transaction = message["transaction"]
        rs, ws = transaction["read_set"], transaction["write_set"]
        abort = False
        with self.lock:
            # Certification Test: Check for obsolete reads
            for read in rs:
                item, read_version = read["item"], read["version"]
                _, current_version = self.data_store.get(item, (None, 0))
                if current_version > read_version:
                    abort = True
                    break

            if abort:
                response = {
                    "type": COMMIT_RESPONSE,
                    "transaction_id": transaction["id"],
                    "status": "ABORT",
                }
                if conn:
                    conn.sendall(
                        serialize(response).encode()
                    )  # Send response if connection exists
                print(
                    f"Server {self.server_id} aborted transaction {transaction['id']}"
                )
            else:
                # Apply the write set
                for write in ws:
                    item, value = write["item"], write["value"]
                    _, current_version = self.data_store.get(item, (None, 0))
                    self.data_store[item] = (value, current_version + 1)
                response = {
                    "type": COMMIT_RESPONSE,
                    "transaction_id": transaction["id"],
                    "status": "COMMIT",
                    "write_set": ws,
                }
                if conn:
                    conn.sendall(
                        serialize(response).encode()
                    )  # Send response if connection exists
                print(
                    f"Server {self.server_id} committed transaction {transaction['id']} with write set: {ws}"
                )
        self.broadcast.broadcast(response)

    def handle_broadcast_message(self, message):
        """Handle broadcast messages."""
        try:
            msg = message.get("message")  # Extract the original message
            if not msg:
                print(f"Server {self.server_id} received invalid broadcast message.")
                return

            msg_type = msg.get("type")
            if msg_type == COMMIT_REQUEST:
                # Treat as a direct commit request and process it
                self.handle_commit_request(
                    None, msg
                )  # Simulate the request without a connection
            elif msg_type == COMMIT_RESPONSE:
                transaction_id = msg["transaction_id"]
                status = msg["status"]
                if status == "COMMIT":
                    ws = msg.get("write_set", [])
                    with self.lock:
                        for write in ws:
                            item, value = write["item"], write["value"]
                            _, current_version = self.data_store.get(item, (None, 0))
                            self.data_store[item] = (value, current_version + 1)
                        print(
                            f"Server {self.server_id} applied COMMIT for transaction {transaction_id} with write set: {ws}"
                        )
                elif status == "ABORT":
                    print(
                        f"Server {self.server_id} acknowledged ABORT for transaction {transaction_id}"
                    )
            else:
                print(f"Unknown broadcast message type: {msg_type}")
        except Exception as e:
            print(f"Error handling broadcast message on server {self.server_id}: {e}")

    def get_data_store(self):
        """Return the current state of the data store."""
        with self.lock:
            return dict(
                self.data_store
            )  # Return a copy of the data store to avoid race conditions
