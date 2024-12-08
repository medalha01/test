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


from queue import PriorityQueue


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

        # Sequencer processing queue
        self.sequence_queue = PriorityQueue()
        self.current_sequence = 0

        # Initialize Paxos components (Proposer, Acceptor, Learner)
        proposer_port = get_paxos_port(server_id)
        accepter_port = proposer_port + 1
        learner_port = proposer_port + 2

        self.peers = [
            (self.host, get_paxos_port(r)) for r in replicas if r != server_id
        ]
        self.proposer = PaxosProposer(server_id, self.host, proposer_port, self.peers)
        self.accepter = PaxosAccepter(server_id, self.host, accepter_port)
        self.learner = PaxosLearner(server_id, self.host, learner_port)

    def handle_commit_request(self, message):
        """Handle commit requests and add them to the processing queue."""
        transaction = message["transaction"]
        sequence_number = message["sequence_number"]
        write_set = transaction["write_set"]
        transaction_id = transaction["id"]

        # Add to sequence queue for ordered processing
        self.sequence_queue.put((sequence_number, transaction_id, write_set))

        # Start a thread to process the queue
        threading.Thread(target=self.process_sequence_queue, daemon=True).start()

    def process_sequence_queue(self):
        """Process transactions from the sequence queue in order."""
        while not self.sequence_queue.empty():
            sequence_number, transaction_id, write_set = self.sequence_queue.get()

            # Ensure sequential order
            if sequence_number != self.current_sequence + 1:
                self.sequence_queue.put((sequence_number, transaction_id, write_set))
                return  # Wait for the correct sequence

            self.current_sequence = sequence_number

            # Start Paxos consensus
            print(
                f"Server {self.server_id} initiating Paxos for transaction {transaction_id}."
            )
            proposed_value = {"transaction_id": transaction_id, "write_set": write_set}
            self.proposer.propose(proposed_value)

            # Wait for consensus
            if self.wait_for_consensus(transaction_id):
                consensus_value = self.learner.learned_values.get(transaction_id)
                if consensus_value and consensus_value["write_set"] == write_set:
                    self.apply_commit(transaction_id, write_set)
                else:
                    print(
                        f"Server {self.server_id} aborting transaction {transaction_id} due to conflicting consensus."
                    )
            else:
                print(f"Server {self.server_id} aborting transaction {transaction_id}.")

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

    def get_data_store(self):
        """Return the current state of the data store."""
        with self.lock:
            return dict(self.data_store)

    def validate_sequence(self, sequence_number):
        """Validate and track sequence numbers for ordered execution."""
        if not hasattr(self, "last_sequence_number"):
            self.last_sequence_number = 0

        if sequence_number == self.last_sequence_number + 1:
            self.last_sequence_number = sequence_number
            return True
        return False

    def wait_for_consensus(self, transaction_id):
        """Wait for consensus from the learner."""
        import time

        timeout = 10  # Maximum wait time in seconds
        elapsed = 0
        while elapsed < timeout:
            with self.lock:
                if transaction_id in self.learner.learned_values:
                    return True  # Consensus achieved
            time.sleep(1)
            elapsed += 1
        return False  # Consensus not achieved

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
