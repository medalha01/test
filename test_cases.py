# test_cases.py

import threading
import time
from client import Transaction
from broadcast import Broadcast, Sequencer

from utils import send_message, deserialize, get_server_port
from server import Server

def test_commit_transaction(client_id, operations, broadcast, servers):
    """Test case where a transaction should commit."""
    transaction = Transaction(client_id, operations, broadcast, servers)
    transaction.execute()

def test_abort_transaction(client_id, operations, broadcast, servers):
    """Test case where a transaction should abort due to obsolete reads."""
    transaction = Transaction(client_id, operations, broadcast, servers)
    transaction.execute()


def run_tests():
    """Run test cases."""
    num_servers = 3

    # Start the Sequencer
    sequencer = Sequencer()  # Ensure Sequencer is defined in broadcast.py
    threading.Thread(target=sequencer.start, daemon=True).start()
    time.sleep(1)  # Allow the sequencer to start

    # Define server replicas
    replicas = [('localhost', get_server_port(i)) for i in range(num_servers)]

    # Start servers
    for i in range(num_servers):
        server = Server(i, replicas)
        threading.Thread(target=server.start, daemon=True).start()
    time.sleep(1)  # Allow servers to start

    # Initialize broadcast
    broadcast = Broadcast(replicas=replicas)

    # Test Case 1: Two transactions writing to different items - both should commit
    operations1 = [{'type': 'READ', 'item': 'x'}, {'type': 'WRITE', 'item': 'x', 'value': 'A'}]
    operations2 = [{'type': 'READ', 'item': 'y'}, {'type': 'WRITE', 'item': 'y', 'value': 'B'}]
    threading.Thread(target=lambda: Transaction(1, operations1, broadcast, replicas).execute(), daemon=True).start()
    threading.Thread(target=lambda: Transaction(2, operations2, broadcast, replicas).execute(), daemon=True).start()

    # Wait for transactions to complete
    time.sleep(5)

if __name__ == "__main__":
    run_tests()
