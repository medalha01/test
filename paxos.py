# paxos.py

import socket
import threading
import json


class PaxosNode:
    def __init__(self, id, ip, port):
        self.id = id
        self.ip = ip
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((ip, port))

    def send_message(self, message, peer):
        print(f"[Node {self.id}] Sending message to {peer}: {message}")
        self.socket.sendto(json.dumps(message).encode(), peer)

    def receive_message(self):
        while True:
            data, addr = self.socket.recvfrom(1024)
            message = json.loads(data.decode())
            print(f"[Node {self.id}] Received message from {addr}: {message}")
            self.on_receive_message(message, addr)

    def on_receive_message(self, message, addr):
        pass


class PaxosProposer(PaxosNode):
    def __init__(self, id, ip, port, peers):
        super().__init__(id, ip, port)
        self.peers = peers
        self.proposal_id = 0
        self.promises_received = 0
        self.proposed_value = None

    def generate_proposal_id(self):
        self.proposal_id += 1
        return f"{self.id}-{self.proposal_id}"

    def propose(self, value):
        self.promises_received = 0
        self.proposed_value = value
        proposal_id = self.generate_proposal_id()
        message = {"type": "proposal", "proposal_id": proposal_id, "value": value}
        print(
            f"[Proposer {self.id}] Proposing value: {value} with proposal ID: {proposal_id}"
        )
        for peer in self.peers:
            self.send_message(message, peer)

    def on_receive_message(self, message, addr):
        if message["type"] == "promise":
            self.promises_received += 1
            print(
                f"[Proposer {self.id}] Received promise {self.promises_received}/{len(self.peers)}"
            )
            if self.promises_received > len(self.peers) // 2:
                accept_message = {
                    "type": "accepted",
                    "proposal_id": message["proposal_id"],
                    "value": self.proposed_value,
                }
                print(f"[Proposer {self.id}] Sending accepted message to peers.")
                for peer in self.peers:
                    self.send_message(accept_message, peer)


class PaxosAccepter(PaxosNode):
    def __init__(self, id, ip, port):
        super().__init__(id, ip, port)
        self.promised_id = None
        self.accepted_value = None

    def on_receive_message(self, message, addr):
        if message["type"] == "proposal":
            proposal_id = message["proposal_id"]
            if self.promised_id is None or proposal_id > self.promised_id:
                self.promised_id = proposal_id
                print(f"[Accepter {self.id}] Promising proposal ID: {proposal_id}")
                response = {"type": "promise", "proposal_id": proposal_id}
                self.send_message(response, addr)
        elif message["type"] == "accept_request":
            proposal_id = message["proposal_id"]
            value = message["value"]
            if self.promised_id is None or proposal_id >= self.promised_id:
                self.promised_id = proposal_id
                self.accepted_value = value
                print(
                    f"[Accepter {self.id}] Accepted value: {value} for proposal ID: {proposal_id}"
                )
                response = {
                    "type": "accepted",
                    "proposal_id": proposal_id,
                    "value": value,
                }
                self.send_message(response, addr)


class PaxosLearner(PaxosNode):
    def __init__(self, id, ip, port):
        super().__init__(id, ip, port)
        self.learned_values = {}

    def on_receive_message(self, message, addr):
        if message["type"] == "accepted":
            proposal_id = message["proposal_id"]
            value = message["value"]
            if proposal_id not in self.learned_values:
                self.learned_values[proposal_id] = value
                print(
                    f"[Learner {self.id}] Learned value: {value} for proposal ID: {proposal_id}"
                )
