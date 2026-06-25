'''This is my take on a Reliable UDP implementation in Python.'''

import threading
from threading import Lock

from collections import deque

import random
import time
import json

class Packet: # Packet Object
    def __init__(self, data, sequence, total):
        self.data = data
        self.sequence = sequence
        self.total = total

        self.ownership = 'SENDER'

    def time(self):
        self.timer = time.time()
    
class FinishedData:
    def __init__(self, data, packets_made : int):
        self.data = data
        self.packets_made : int = packets_made

class NetworkSimulation: # Simulation Class (Simulating Socket Library!)
    def __init__(self, simulated_delay_max_chance :int = 10, simulated_loss_max_chance :int = 10): 
        self.delayed : deque[Packet] = deque()

        self.in_transit_to_server : deque[Packet] = deque()
        self.in_transit_to_client : deque[Packet] = deque()

        self.simulated_delay_max_chance = simulated_delay_max_chance
        self.simulated_loss_max_chance = simulated_loss_max_chance

    def send(self, packet : Packet):

        if random.randint(1,self.simulated_loss_max_chance) == 1: # Packet Loss
            return

        if random.randint(1,self.simulated_delay_max_chance) == 1: # Packet Delay
            self.delayed.append(packet)
            return 

        if packet.ownership == 'SENDER':
            self.in_transit_to_server.append(packet)
        else:
            self.in_transit_to_client.append(packet)

    def tick(self):
        if self.delayed:
            p = self.delayed.popleft()
            if p.ownership == 'SENDER':
                self.in_transit_to_server.append(p)
            else:
                self.in_transit_to_client.append(p)
        
        # clear buffer in case of over fill
        if len(self.in_transit_to_server) > 20:
            self.in_transit_to_server.clear()
        if len(self.in_transit_to_client) > 20:
            self.in_transit_to_client.clear()

    def recv(self, ownership):
        if not self.in_transit_to_server and not self.in_transit_to_client:
            return None
        
        if ownership == 'SENDER':
            if self.in_transit_to_server:
                return self.in_transit_to_server.popleft()
        else:
            if self.in_transit_to_client:
                return self.in_transit_to_client.popleft()

class ReliableUDPReceiver:
    def __init__(self, Network: NetworkSimulation, tick_time: float = 0.1, CHUNK_SIZE : int = 32):
        '''Initialize the ReliableUDPReceiver with a network simulation. Optional parameters include tick time and chunk size.'''
        self.CHUNK_SIZE : int = CHUNK_SIZE

        self.UNORGANIZED_RECEIVED : list[Packet] = []
        self.RECEIVING_BUFFER : dict[int, Packet] = {}

        self.ON_GOING = False
        self.CURRENT_SEQUENCE = 1

        self.SENDING_BUFFER : list[Packet] = []
        self.AWAITING_COLLECT_BUFFER : list[FinishedData] = []
        
        self.Network = Network        
        self.Lock = Lock()

        self.RESEND_TIMER = 0.5
        self.tick_time = tick_time
    
    def finalize(self): # Once RECEIVING_BUFFER is full, this will rebuild data, and put it into AWAITING_COLLECT_BUFFER for user to take. Also, cleanup for buffers.
        if dict(self.RECEIVING_BUFFER):
            data = b''
            for seq in sorted(self.RECEIVING_BUFFER.keys()):
                data += self.RECEIVING_BUFFER[seq].data
            
            if len(self.RECEIVING_BUFFER) == self.RECEIVING_BUFFER[list(self.RECEIVING_BUFFER.keys())[-1]].total:
                self.AWAITING_COLLECT_BUFFER.append(FinishedData(data, packets_made=len(self.RECEIVING_BUFFER)))
            else:
                #print("Error: Incomplete data received.")
                while len(self.RECEIVING_BUFFER) < self.RECEIVING_BUFFER[list(self.RECEIVING_BUFFER.keys())[-1]].total:
                    self.NES_ACK() # Replace in future for variable SWP

            self.RECEIVING_BUFFER.clear()

        self.SENDING_BUFFER.clear()
        self.CURRENT_SEQUENCE = 1

    def find_missing_seq(self, total : int):
        if not self.RECEIVING_BUFFER:
            return None

        # Reverse order to find the first missing sequence
        expected_seq = total
        while expected_seq in self.RECEIVING_BUFFER:
            expected_seq -= 1

        return expected_seq

    def SEL_REP_ACK(self): 
        '''SEL_REP (Selective Repeat) ACK SWP (Sliding Window Protocol)\n
        This method is used to acknowledge the next expected sequence number.
        Initial packet stream is acknowledged at the end, if any are missing, the sender will retransmit only the missing packets.

        This method is the most efficient and widely used in practice.

        Currently not implemented lmao.
        '''
        pass

    def NES_ACK(self): 
        '''Next Expected Sequence ACK SWP (Sliding Window Protocol)\n
        This method is used to acknowledge the next expected sequence number.
        If packet is out-of-order, it will notify the sender of the missing sequence.
        This will continue until all packets are received in order.

        This method is more complex, and is what I invented as a hybrid between ONE-BIT and SEL_REP SWP styles.
        '''
        self.ON_GOING = True

        if self.UNORGANIZED_RECEIVED:
            packet = self.UNORGANIZED_RECEIVED.pop(0)
            self.RECEIVING_BUFFER[packet.sequence] = packet

            if packet.sequence == self.CURRENT_SEQUENCE:
                self.CURRENT_SEQUENCE += 1

                check = True
                while check:
                    if self.CURRENT_SEQUENCE in self.RECEIVING_BUFFER:
                        self.CURRENT_SEQUENCE += 1
                    else:
                        check = False
                    
            elif self.CURRENT_SEQUENCE-1 == packet.total:
                #print(self.CURRENT_SEQUENCE, packet.total)
                
                FinishACK = Packet('ACK', self.RECEIVING_BUFFER[list(self.RECEIVING_BUFFER.keys())[-1]].sequence, self.RECEIVING_BUFFER[list(self.RECEIVING_BUFFER.keys())[-1]].total)
                FinishACK.ownership = 'RECEIVER'
                self.Network.send(FinishACK)

                self.ON_GOING = False
            else:
                #print("out of order", self.CURRENT_SEQUENCE-1, packet.sequence, packet.total)

                OutOfOrderACK = Packet('ACK', packet.sequence, packet.total)
                OutOfOrderACK.ownership = 'RECEIVER'
                self.Network.send(OutOfOrderACK)
            
            
        if self.ON_GOING:
            if self.RECEIVING_BUFFER:
                if time.time() - self.RECEIVING_BUFFER[list(self.RECEIVING_BUFFER.keys())[-1]].timer > self.RESEND_TIMER:

                    TimerACK = Packet('ACK', self.RECEIVING_BUFFER[list(self.RECEIVING_BUFFER.keys())[-1]].sequence, self.RECEIVING_BUFFER[list(self.RECEIVING_BUFFER.keys())[-1]].total)
                    TimerACK.ownership = 'RECEIVER'
                    self.Network.send(TimerACK)
                
                if self.CURRENT_SEQUENCE == self.RECEIVING_BUFFER[list(self.RECEIVING_BUFFER.keys())[-1]].total:
                    self.ON_GOING = False

                    FinishACK = Packet('ACK', self.RECEIVING_BUFFER[list(self.RECEIVING_BUFFER.keys())[-1]].sequence, self.RECEIVING_BUFFER[list(self.RECEIVING_BUFFER.keys())[-1]].total)
                    FinishACK.ownership = 'RECEIVER'
                    self.Network.send(FinishACK)
                
                else:
                    pass
            
        else:
            self.finalize()
        
    def receive_thread(self): 
        while True:
            time.sleep(self.tick_time)
            with self.Lock:
                data = self.Network.recv('SENDER')
                
                if data and data.ownership != 'RECEIVER':
                    self.UNORGANIZED_RECEIVED.append(data)
                    self.NES_ACK()
    
    def receive(self):
        with self.Lock:
            if self.AWAITING_COLLECT_BUFFER:
                return self.AWAITING_COLLECT_BUFFER.pop(0)
            return None

    def run(self):
        '''Start the RUDPReceiver threads. (Which include simply receiving)'''
        threading.Thread(target=self.receive_thread, daemon=True).start()

class ReliableUDPSender:
    def __init__(self, Network: NetworkSimulation, tick_time: float = 0.1, CHUNK_SIZE : int = 32):
        '''Initialize the ReliableUDPSender with a network simulation. Optional parameters include tick time and chunk size.'''
        self.CHUNK_SIZE : int = CHUNK_SIZE

        self.SENDING_BUFFER : list[Packet] = []
        self.IN_TRANSIT : dict[int, Packet] = {}
        
        self.Network = Network
        self.Lock = Lock()

        self.tick_time = tick_time
    
    def finalize(self):
        self.IN_TRANSIT.clear()

    def receive_thread(self):
        while True:
            time.sleep(self.tick_time)
            data = self.Network.recv('RECEIVER')
            
            if data and data.ownership != 'SENDER':
                if data.data == 'ACK':
                    if data.sequence == data.total:
                        self.finalize()
                    else: 
                        if self.IN_TRANSIT:
                            missing_packet = self.IN_TRANSIT[list(self.IN_TRANSIT.keys())[-1]]
                            missing_packet.time()
                            self.Network.send(missing_packet)
            
    def pack(self, data) -> list[Packet]:
        chunks : list = []
        packets : list[Packet] = []

        if type(data) != str:
            data = json.dumps(data)
        data = data.encode('UTF-8')

        for i in range(0, len(data), self.CHUNK_SIZE):
            single_chunk = data[i:i+self.CHUNK_SIZE]
            chunks.append(single_chunk)

        for seq, p in enumerate(chunks):
            single_packet = Packet(p, seq+1, len(chunks))
            packets.append(single_packet)

        return packets
    
    def send_thread(self):
        while True:
            time.sleep(self.tick_time)
            with self.Lock:
                for i, data in enumerate(list(self.SENDING_BUFFER)):

                    for packet in self.pack(data):
                        packet.time()

                        self.Network.send(packet)
                        self.IN_TRANSIT[packet.sequence] = packet
                    
                    if i == len(self.SENDING_BUFFER) - 1:
                        del self.SENDING_BUFFER[i]

    def send(self, data):
        with self.Lock:
            self.SENDING_BUFFER.append(data)

    def run(self):
        '''Start the RUDPSender threads. (Which include sending and receiving)'''
        threading.Thread(target=self.send_thread, daemon=True).start()
        threading.Thread(target=self.receive_thread, daemon=True).start()

def run():
    running = True

    Network = NetworkSimulation()
    
    RUDP_Sender = ReliableUDPSender(Network, tick_time=0.005, CHUNK_SIZE=32) # For the person reading, this is a small chunk size for testing packets made. (Standard is 1024)
    RUDP_Receiver = ReliableUDPReceiver(Network, tick_time=0.005, CHUNK_SIZE=32)

    RUDP_Receiver.run()
    RUDP_Sender.run()

    while running:
        time.sleep(0.1)

        Network.tick()

        RUDP_Sender.send("Hello, World! This is a test message. This message automatically gets split into many packets, and the receiver should be able to reassemble them correctly.")

        received_data = RUDP_Receiver.receive()
        if received_data:
            print(f"Received: {received_data.data} | Packets made: {received_data.packets_made}")

if __name__ == '__main__':
    run()