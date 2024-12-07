import threading
import time
from server import Server
from client import Transaction
from broadcast import Sequencer, Broadcast
from utils import SERVER_HOST, SERVER_BASE_PORT, CLIENT_HOST, CLIENT_BASE_PORT

# List to keep server instances globally for inspection
server_instances = []

def start_sequencer():
    """Start the sequencer in a separate thread."""
    sequencer = Sequencer()
    threading.Thread(target=sequencer.start, daemon=True).start()
    print("Sequencer started.")

def start_servers(num_servers):
    """Start multiple server instances."""
    global server_instances
    replicas = [('localhost', SERVER_BASE_PORT + i) for i in range(num_servers)]
    servers = []
    for i in range(num_servers):
        server = Server(server_id=i, replicas=replicas)
        threading.Thread(target=server.start, daemon=True).start()
        servers.append(server)
        print(f"Server {i} started on {server.host}:{server.port}")
    server_instances = servers  # Store server instances globally
    return replicas

def start_clients(client_id, operations, broadcast, servers):
    """Start a client and execute a transaction."""
    transaction = Transaction(client_id=client_id, operations=operations, broadcast=broadcast, servers=servers)
    print(f"Client {client_id} started transaction with operations: {operations}")
    transaction.execute()

def display_server_data_stores():
    """Display the data stores of all servers."""
    global server_instances
    print("\nServer Data Stores:")
    for server in server_instances:
        data_store = server.get_data_store()
        print(f"Server {server.server_id}: {data_store}")
        if not data_store:
            print(f"Server {server.server_id} data store is empty. Possible issue with commit or broadcast.")

def main():
    """Main function to start sequencer, servers, and clients."""
    num_servers = 3
    num_clients = 2

    # Start sequencer
    start_sequencer()
    time.sleep(1)  # Allow sequencer to start

    # Start servers
    replicas = start_servers(num_servers)
    time.sleep(1)  # Allow servers to start

    # Initialize broadcast
    broadcast = Broadcast(replicas=replicas)

    # Define client operations
    client_operations = [
        [{'type': 'READ', 'item': 'x'}, {'type': 'WRITE', 'item': 'x', 'value': 'A'}],
        [{'type': 'READ', 'item': 'x'}, {'type': 'WRITE', 'item': 'x', 'value': 'B'}]
    ]

    # Start clients
    threads = []
    for client_id in range(num_clients):
        operations = client_operations[client_id]
        client_thread = threading.Thread(target=start_clients, args=(client_id, operations, broadcast, replicas), daemon=True)
        client_thread.start()
        threads.append(client_thread)

    # Wait for clients to finish
    for thread in threads:
        thread.join()

    # Let the system propagate changes
    time.sleep(5)

    # Display the state of the servers' data stores
    display_server_data_stores()

if __name__ == '__main__':
    main()

