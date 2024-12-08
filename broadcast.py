# broadcast.py

import socket
import threading
from utils import (
    serialize,
    deserialize,
    SEQUENCER_HOST,
    SEQUENCER_PORT,
    BROADCAST_MESSAGE,
)


class Sequencer:
    """Sequencer for atomic broadcast to ensure total order."""

    def __init__(self, host=SEQUENCER_HOST, port=SEQUENCER_PORT):
        self.host = host
        self.port = port
        self.sequence_number = 0
        self.lock = threading.Lock()

    def start(self):
        """Start the sequencer server."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen()
        print(f"Sequencer listening on {self.host}:{self.port}")
        while True:
            conn, addr = server.accept()
            threading.Thread(target=self.handle_client, args=(conn, addr)).start()

    def handle_client(self, conn, addr):
        """Handle incoming broadcast requests."""
        try:
            message = deserialize(conn.recv(4096).decode())
            if message and message["type"] == "SEQUENCER_REQUEST":
                with self.lock:
                    self.sequence_number += 1
                    seq_num = self.sequence_number
                response = {
                    "type": "SEQUENCER_RESPONSE",
                    "sequence_number": seq_num,
                    "message": message["message"],
                }
                conn.sendall(serialize(response).encode())
                print(
                    f"Sequencer assigned sequence number {seq_num} to message from {addr}"
                )
        except Exception as e:
            print(f"Error in sequencer handling client {addr}: {e}")
        finally:
            conn.close()


class Broadcast:
    """Client-side broadcaster using sequencer for atomic broadcast."""

    def __init__(
        self, sequencer_host=SEQUENCER_HOST, sequencer_port=SEQUENCER_PORT, replicas=[]
    ):
        self.sequencer_host = sequencer_host
        self.sequencer_port = sequencer_port
        self.replicas = replicas  # List of (host, port) tuples

    def broadcast(self, message):
        """Broadcast a message to all replicas via sequencer."""
        try:
            request = {"type": "SEQUENCER_REQUEST", "message": message}
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.sequencer_host, self.sequencer_port))
                s.sendall(serialize(request).encode())
                response = deserialize(s.recv(4096).decode())
            if response["type"] == "SEQUENCER_RESPONSE":
                seq_num = response["sequence_number"]
                ordered_message = {
                    "type": BROADCAST_MESSAGE,  # Ensure the type is BROADCAST_MESSAGE
                    "sequence_number": seq_num,
                    "message": response[
                        "message"
                    ],  # Include the original COMMIT_REQUEST or COMMIT_RESPONSE
                }
                for replica in self.replicas:
                    self.send_to_replica(replica, ordered_message)
                print(f"Broadcasted message with sequence number {seq_num}")
            else:
                print("Invalid response from sequencer.")
        except Exception as e:
            print(f"Failed to broadcast message: {e}")

    def send_to_replica(self, replica, message):
        """Send a message to a replica."""
        host, port = replica  # Ensure replica is a tuple
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((host, port))
                    s.sendall(serialize(message).encode())
                print(f"Message sent to replica {replica} successfully.")
                return
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for replica {replica}: {e}")
        print(
            f"Failed to deliver message to replica {replica} after {max_retries} attempts."
        )
