import sys
import socket
import pickle
import os.path
import threading

import config
import json


class ChunkServer():
    def __init__(self, host, port, rootdir):
        self.host = host
        self.port = port
        self.rootdir = rootdir
        self.present = {}
        self.lock = threading.Lock()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))

    def listen(self):
        self.sock.listen(5)
        while True:
            client, address = self.sock.accept()
            handler = threading.Thread(
                target=self.service, args=(client, address))
            handler.start()

    def service(self, client, address):
        request = client.recv(config.MESSAGE_SIZE)
        message = json.loads(request.decode('utf-8'))
        print(message)
        sender_type = message["sender_type"]
        command = message["function"]
        args = message["args"]

        if command == "write_chunk":
            self.write_chunk(client, args)
        
    def write_chunk(self, client, args):
        print(f"Writing Chunk {args[0]}")
        # recieve data from client as a stream (sendall)
        data = b''
        while True:
            packet = client.recv(config.PACKET_SIZE)
            if not packet:
                break
            data += packet
        data = data.decode('utf-8').strip()
        # write the data, creating the file if it does not exist
        os.makedirs(os.path.dirname(os.path.join(self.rootdir, args[0])), exist_ok=True)
        with open(os.path.join(self.rootdir, args[0]), 'w') as f:
            f.write(data)

        client.close()





    def _respond_status(self, code, message):

        response =  {
			'status': code,
			'message': message
		}
        response = json.dumps(response).encode('utf-8')
        response += b' ' * (config.MESSAGE_SIZE - len(response))
        return response

if __name__ == '__main__':
    num = int(sys.argv[1])
    if num < 0 or num > config.NUM_CHUNKS-1:
        print(f'Enter a valid Port number in [0, {config.NUM_CHUNKS-1}]')
        exit()

    port = config.CHUNK_PORTS[num]
    rootdir = config.ROOT_DIR[num]

    print('Chunk Server Running')
    cs = ChunkServer(config.HOST, port, rootdir)
    cs.listen()


