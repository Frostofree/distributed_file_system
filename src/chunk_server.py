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
        self.sock.bind((self.host, self.port))

    def listen(self):
        self.sock.listen(5)
        while True:
            client, address = self.sock.accept()
            handler = threading.Thread(
                target=self.service, args=(client, address))
            handler.start()

    def service(self, client, address):
        ip, port = address
        try:
            request = client.recv(config.MESSAGE_SIZE)
            if request == b'':
                raise Exception(f'client {ip}:{address} disconnected')
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
                elif command == 'delete_chunk':
                    self.delete_chunk(client, args)
                if command == 'replicate_chunk':
                    self.replicate_chunk(client, args)
                
            elif sender_type == 'chunk_server':
                if command == "write_chunk":
                    self.write_chunk(client, args)

        except Exception as e:
            print(e)
            client.close()
                

    def heartbeart_handler(self, client, args):
        response = self._respond_status(0, 'OK')
        client.sendall(response)

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
        try:
            os.remove(file_path)
            client.send(self._respond_status(0, 'Chunk deleted'))
        except Exception as e:
            print(e)
            client.send(self._respond_status(1, 'Chunk not found'))
        


    def write_chunk(self, client, args):
        # recieve data from client as a stream (sendall)
        data = b''
        while True:
            packet = client.recv(config.PACKET_SIZE)
            if not packet:
                break
            data += packet
        data = data.decode('utf-8').strip()
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
    
    def _response_message(self, code):

        response =  {
			'status': code,
		}
        response = json.dumps(response).encode('utf-8')
        response += b' ' * (config.MESSAGE_SIZE - len(response))
        return response

    def _get_message_data(self, function, *args):
        function = function
        message = {
            'sender_type': 'chunk_server',
            'function': function,
            'args': args
        }

        # encode the message in utf8
        encoded = json.dumps(message).encode('utf-8')
        encoded += b' ' * (config.MESSAGE_SIZE - len(encoded))
        return encoded

    def read_chunk2(self, id):
        data = ''
        with open(os.path.join(self.rootdir, id), 'rb') as f:
            data = f.read()

        return data
    
    # recieve on chunk server side and handle final reply to master in master.py 
    def send_chunk_data_to_new_chunk_server(self, chunk_id, new_chunk_loc):
        data = self.read_chunk2(chunk_id)
        try:
            new_chunk_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            new_chunk_server.connect((socket.gethostbyname('localhost'), new_chunk_loc))
            # new_chunk_server.settimeout(1)
            request = self._get_message_data('write_chunk', chunk_id)	
            new_chunk_server.sendall(request)
            new_chunk_server.sendall(data)
        except socket.timeout:
            print("Socket operation timed out on sending data corresponding to chunk_id: ", chunk_id)
            return 0
        except socket.error:
            print("Socket error occured.")
            return 0
        except Exception as e:
            print("Chunk to chunk message passing failed")
            print("With error: ", e)
            return 0
        return 1

    def replicate_chunk(self, client, args):
        dict_data = args[0] # {chunk_id, new_chunk_loc}
        try:
            # status = 1
            status = self.send_chunk_data_to_new_chunk_server(dict_data["chunk_id"], dict_data["new_chunk_loc"])
            # time.sleep(5)
            if(status == 0):
                print("Status 0 returned. Something went wrong")
                # return 0 to master
                response = self._response_message(0)
            # reply 1 to master
            else:
                response = self._response_message(1)
        except:
            print("Something went wrong.")
            response = self._response_message(0)
            # return 0 to master
        
        try:
            client.sendall(response)
        except:
            print("Sending reply to master failed")


if __name__ == '__main__':
    num = int(sys.argv[1])
    if num < 0 or num > config.NUM_CHUNKS-1:
        print(f'Enter a valid Port number in [0, {config.NUM_CHUNKS-1}]')
        exit()

    port = config.CHUNK_PORTS[num]
    rootdir = config.ROOT_DIR[num]
    os.system('clear')
    print('Chunk Server Running')
    cs = ChunkServer(config.HOST, port, rootdir)
    cs.listen()