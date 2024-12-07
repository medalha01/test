# paxos.py
import socket
import threading
import pickle

class PaxosNode:
    """Implements Proposer, Acceptor, and Learner roles for Paxos."""
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers  # List of (host, port) tuples for other nodes
        self.n_promised = -1  # Highest proposal number promised
        self.n_accepted = -1  # Highest proposal number accepted
        self.value_accepted = None  # Value associated with the accepted proposal
        self.chosen_value = None  # Final chosen value (Learner)
        self.lock = threading.Lock()

    def start(self):
        """Start listening for Paxos messages."""
        listener = threading.Thread(target=self.listen, daemon=True)
        listener.start()

    def listen(self):
        """Listen for incoming Paxos messages."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host, port = self.peers[self.node_id]
        server_socket.bind((host, port))
        server_socket.listen()
        print(f"Paxos Node {self.node_id} listening on {host}:{port}")

        while True:
            conn, addr = server_socket.accept()
            threading.Thread(target=self.handle_message, args=(conn,)).start()

    def handle_message(self, conn):
        """Handle incoming Paxos messages."""
        try:
            message = pickle.loads(conn.recv(4096))
            print(f"Node {self.node_id} received {message['type']} from Node {message.get('from')}")
            msg_type = message['type']
            if msg_type == 'PREPARE':
                self.handle_prepare(message, conn)
            elif msg_type == 'PROMISE':
                self.handle_promise(message)
            elif msg_type == 'ACCEPT':
                self.handle_accept(message, conn)
            elif msg_type == 'ACCEPTED':
                self.handle_accepted(message)
            elif msg_type == 'DECIDE':
                self.handle_decide(message)
        except Exception as e:
            print(f"Error handling message: {e}")
        finally:
            conn.close()


    def handle_prepare(self, message, conn):
        """Handle a PREPARE message."""
        proposal_number = message['proposal_number']
        with self.lock:
            if proposal_number > self.n_promised:
                self.n_promised = proposal_number
                response = {
                    'type': 'PROMISE',
                    'from': self.node_id,
                    'proposal_number': proposal_number,
                    'accepted_number': self.n_accepted,
                    'accepted_value': self.value_accepted,
                }
                conn.sendall(pickle.dumps(response))
                print(f"Node {self.node_id} sent PROMISE for proposal {proposal_number}")


    def handle_promise(self, message):
        """Handle a PROMISE message."""
        print(f"Node {self.node_id} received PROMISE: {message}")

    def handle_accept(self, message, conn):
        """Handle an ACCEPT message."""
        proposal_number = message['proposal_number']
        value = message['value']
        with self.lock:
            if proposal_number >= self.n_promised:
                self.n_promised = proposal_number
                self.n_accepted = proposal_number
                self.value_accepted = value
                response = {
                    'type': 'ACCEPTED',
                    'from': self.node_id,
                    'proposal_number': proposal_number,
                    'value': value,
                }
                conn.sendall(pickle.dumps(response))
                print(f"Node {self.node_id} sent ACCEPTED for proposal {proposal_number}")

                # Broadcast DECIDE if value is accepted
                if self.n_promised == proposal_number:
                    decide_message = {
                        'type': 'DECIDE',
                        'from': self.node_id,
                        'value': value,
                    }
                    for i in range(len(self.peers)):
                        self.send_message(i, decide_message)


    def handle_accept(self, message, conn):
        """Handle an ACCEPT message."""
        proposal_number = message['proposal_number']
        value = message['value']
        with self.lock:
            if proposal_number >= self.n_promised:
                self.n_promised = proposal_number
                self.n_accepted = proposal_number
                self.value_accepted = value
                response = {
                    'type': 'ACCEPTED',
                    'from': self.node_id,
                    'proposal_number': proposal_number,
                    'value': value,
                }
                conn.sendall(pickle.dumps(response))
                print(f"Node {self.node_id} sent ACCEPTED for proposal {proposal_number}")

                # Send DECIDE only if the value is not yet chosen
                if self.chosen_value is None:
                    self.chosen_value = value
                    decide_message = {
                        'type': 'DECIDE',
                        'from': self.node_id,
                        'value': value,
                    }
                    for i in range(len(self.peers)):
                        if i != self.node_id:  # Avoid sending to self
                            self.send_message(i, decide_message)


    def handle_decide(self, message):
        """Handle a DECIDE message."""
        value = message['value']
        with self.lock:
            # Only process if the value has not been learned
            if self.chosen_value is None:
                self.chosen_value = value
                print(f"Node {self.node_id} learned DECIDED value: {value}")

                # Broadcast DECIDE to peers to ensure propagation
                decide_message = {
                    'type': 'DECIDE',
                    'from': self.node_id,
                    'value': value,
                }
                for i in range(len(self.peers)):
                    if i != self.node_id:  # Avoid sending to self
                        self.send_message(i, decide_message)



    def send_message(self, to, message):
        """Send a Paxos message to another node."""
        try:
            if isinstance(to, tuple):
                host, port = to
            else:
                host, port = self.peers[to]

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
                s.sendall(pickle.dumps(message))
            print(f"Node {self.node_id} sent {message['type']} to Node {to}")
        except Exception as e:
            print(f"Failed to send message to Node {to}: {e}")



    def propose(self, value):
        """Propose a value as a Proposer."""
        proposal_number = self.node_id  # Use node ID as proposal number for simplicity
        print(f"Node {self.node_id} proposing value: {value} with proposal number: {proposal_number}")

        prepare_message = {
            'type': 'PREPARE',
            'from': self.node_id,
            'proposal_number': proposal_number,
        }
        for i in range(len(self.peers)):  # Iterate by index
            self.send_message(i, prepare_message)

        # After receiving PROMISE, send ACCEPT
        accept_message = {
            'type': 'ACCEPT',
            'from': self.node_id,
            'proposal_number': proposal_number,
            'value': value,
        }
        for i in range(len(self.peers)):  # Iterate by index
            self.send_message(i, accept_message)


