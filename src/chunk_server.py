import sys
import socket
import threading

import config

class ChunkServer():
	def __init__(self, host, port):
		self.host = host
		self.port = port
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR)
		self.sock.bind((self.host, self.port))

	def listen(self):
		self.sock.listen(5)
		while True:
			client, address = self.sock.accept()
			client.timeout(60)
			handler = threading.Thread(target=self.service, args=(client, address))
			handler.start()

	def service(self, client, address):
		# listen here fron client and master
		pass


if __name__ == '__main__':
	num = int(sys.argv[1])
	if num < 0 and num > 3:
		print(f'Enter a valid Port number in [0, {config.NUM_CHUNKS-1}]')
		exit()
	port = config.CHUNK_PORTS[num]

	print('Chunk Server Running')
	cs = ChunkServer(config.HOST, port)
	cs.listen()
