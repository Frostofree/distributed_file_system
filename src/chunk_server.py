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
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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

        sender_type = message["sender_type"]
        command = message["function"]
        args = message["args"]

        if sender_type == 'client':
            if command == "write_chunk":
                self.write_chunk(client, args)
            elif command == 'read_chunk':
                self.read_chunk(client, args)
            elif command == 'delete_chunk':
                self.delete_chunk(client, args)
        elif sender_type == 'master':
            if command == 'heartbeat':
                self.heartbeart_handler(client, args)

    def heartbeart_handler(self, client, args):
        response = self._respond_status(0, 'OK')
        client.sendall(response)
        client.close()

    def read_chunk(self, client, args):
        data = ''
        with open(os.path.join(self.rootdir, args[0]), 'r') as f:
            data = f.read()

        response = {
            'status': 0,
            'data': data
        }

        response = json.dumps(response).encode('utf-8')
        response += b' ' * (config.MESSAGE_SIZE - len(response))
        client.send(response)


    def delete_chunk(self, client, args):
        file_path = os.path.join(self.rootdir, args[0])
        os.remove(file_path)
        client.send(self._respond_status(0, 'Chunk deleted'))
    

        
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