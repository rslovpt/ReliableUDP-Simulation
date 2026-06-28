'''This is my take on a Reliable UDP implementation in Python.'''

import threading
from threading import Lock

from collections import deque

import random
import time
import json
from os import system

class Packet: # Packet Object
    def __init__(self, data, sequence, total):
        self.data = data
        self.sequence = sequence
        self.total = total

        self.ownership = 'SENDER'

    def time_start(self):
        self.timer = time.time()
    
class FinishedData: # Finished Data Object (Template for final received data)
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
        self.CURRENT_METHOD : str = 'NES_ACK' 

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
                while len(self.RECEIVING_BUFFER) < self.RECEIVING_BUFFER[list(self.RECEIVING_BUFFER.keys())[-1]].total:
                    getattr(self, self.CURRENT_METHOD)

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
        '''Selective Repeat ACK SWP (Sliding Window Protocol)\n
        This method will send a ACK to each packet received.
        If sender does not receive all the ACK's after timer passed, it will retransmit missing packets.

        This method is the most efficient and widely used in practice.

        This is sender dependent. Sender is responsible for retransmitting missing packets. ACK is sent every time a packet is received.
        '''
        self.ON_GOING = True

        if self.UNORGANIZED_RECEIVED:
            packet = self.UNORGANIZED_RECEIVED.pop(0)
            self.RECEIVING_BUFFER[packet.sequence] = packet

            if len(self.RECEIVING_BUFFER) == packet.total:
                self.ON_GOING = False
            
            PacketACK = Packet('ACK', packet.sequence, packet.total)
            PacketACK.ownership = 'RECEIVER'
            self.Network.send(PacketACK)
        
        if not self.ON_GOING:
            self.finalize()

    def NES_ACK(self): 
        '''Next Expected Sequence ACK SWP (Sliding Window Protocol)\n
        This method is used to acknowledge the next expected sequence number.
        If packet is out-of-order, it will notify the sender of the missing sequence, and once packet is received, current sequence will proceed.
        This will continue until all packets are received in order.

        This method is more complex, and is what I invented as a hybrid between ONE-BIT and SEL_REP SWP styles.
        
        This is receiver dependent. It opts in for the least amount of ACKs needed to be reliable.
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
                FinishACK = Packet('ACK', self.RECEIVING_BUFFER[list(self.RECEIVING_BUFFER.keys())[-1]].sequence, self.RECEIVING_BUFFER[list(self.RECEIVING_BUFFER.keys())[-1]].total)
                FinishACK.ownership = 'RECEIVER'
                self.Network.send(FinishACK)

                self.ON_GOING = False
            else:

                OutOfOrderACK = Packet('ACK', packet.sequence, packet.total)
                OutOfOrderACK.ownership = 'RECEIVER'
                self.Network.send(OutOfOrderACK)
            
            
        if self.ON_GOING:
            if self.RECEIVING_BUFFER:
                if time.time() - self.RECEIVING_BUFFER[list(self.RECEIVING_BUFFER.keys())[-1]].timer > self.RESEND_TIMER:
                    TimerACK = Packet('ACK', self.find_missing_seq(self.RECEIVING_BUFFER[list(self.RECEIVING_BUFFER.keys())[-1]].total), self.RECEIVING_BUFFER[list(self.RECEIVING_BUFFER.keys())[-1]].total)
                    TimerACK.ownership = 'RECEIVER'
                    self.Network.send(TimerACK)
        else:
            self.finalize()
        
    def receive_thread(self): 
        while True:
            time.sleep(self.tick_time)
            with self.Lock:
                data = self.Network.recv('SENDER')
                
                if data and data.ownership != 'RECEIVER':
                    self.UNORGANIZED_RECEIVED.append(data)
                    getattr(self, self.CURRENT_METHOD)()

    def receive(self):
        with self.Lock:
            if self.AWAITING_COLLECT_BUFFER:
                return self.AWAITING_COLLECT_BUFFER.pop(0)
            return None
        
    def Set_ACK_Mode(self, ACK_Mode : int):
        if ACK_Mode == 0:
            self.CURRENT_METHOD = 'NES_ACK'
        elif ACK_Mode == 1:
            self.CURRENT_METHOD = 'SEL_REP_ACK'

    def run(self):
        '''Start the RUDPReceiver threads. (Which include simply receiving)'''
        threading.Thread(target=self.receive_thread, daemon=True).start()

class ReliableUDPSender:
    def __init__(self, Network: NetworkSimulation, tick_time: float = 0.1, CHUNK_SIZE : int = 32):
        '''Initialize the ReliableUDPSender with a network simulation. Optional parameters include tick time and chunk size.'''
        self.CHUNK_SIZE : int = CHUNK_SIZE
        self.CURRENT_METHOD : str = ''
        self.SENDER_DEPENDENT = False

        self.SENDING_BUFFER : list[Packet] = []
        self.IN_TRANSIT : dict[int, Packet] = {}
        
        self.Network = Network
        self.Lock = Lock()

        self.tick_time = tick_time
        self.RESEND_TIMER = 0.5
    
    def finalize(self):
        self.IN_TRANSIT.clear()

    def ACK_HANDLE(self, data : Packet):
        if self.CURRENT_METHOD == 'NES_ACK':
            if data.sequence == data.total:
                self.finalize()
            else: 
                if self.IN_TRANSIT:
                    missing_packet = self.IN_TRANSIT[list(self.IN_TRANSIT.keys())[-1]]
                    missing_packet.time_start()
                    self.Network.send(missing_packet)
        elif self.CURRENT_METHOD == 'SEL_REP_ACK':
            if self.IN_TRANSIT:
                if data.sequence-1 in self.IN_TRANSIT:
                    del self.IN_TRANSIT[data.sequence-1]

    def receive_thread(self):
        while True:
            time.sleep(self.tick_time)
            data = self.Network.recv('RECEIVER')
            
            if data and data.ownership != 'SENDER':
                if data.data == 'ACK':
                    self.ACK_HANDLE(data)
            
            if self.SENDER_DEPENDENT and self.IN_TRANSIT:
                if time.time() - self.IN_TRANSIT[list(self.IN_TRANSIT.keys())[0]].timer > self.RESEND_TIMER:
                    for _, re_packet in self.IN_TRANSIT.items():
                        re_packet : Packet

                        re_packet.time_start()
                        self.Network.send(re_packet)
            
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
                        packet.time_start()

                        self.Network.send(packet)
                        self.IN_TRANSIT[packet.sequence] = packet
                    
                    if i == len(self.SENDING_BUFFER) - 1:
                        del self.SENDING_BUFFER[i]
                
    def send(self, data):
        with self.Lock:
            self.SENDING_BUFFER.append(data)

    def Set_ACK_Mode(self, ACK_Mode : int):
        self.SENDER_DEPENDENT = False

        if ACK_Mode == 0:
            self.CURRENT_METHOD = 'NES_ACK'
        elif ACK_Mode == 1:
            self.CURRENT_METHOD = 'SEL_REP_ACK'
            self.SENDER_DEPENDENT = True

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

    mode = 0 # 0 = NES_ACK, 1 = SEL_REP_ACK
    RUDP_Receiver.Set_ACK_Mode(mode) 
    RUDP_Sender.Set_ACK_Mode(mode)

    count = 0 # for testing purposes

    while running:
        time.sleep(0.1)

        Network.tick()

        RUDP_Sender.send("Hello, World! This is a test message. This message automatically gets split into many packets, and the receiver should be able to reassemble them correctly.")

        received_data = RUDP_Receiver.receive()
        if received_data:
            def terminal():
                nonlocal count
                count += 1
                system("clear")
                print(f"Received: {received_data.data} {count}x | Packets made: {received_data.packets_made}")
            terminal()

if __name__ == '__main__':
    run()