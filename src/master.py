import socket
import threading

import config
import uuid as uuid
import random


class MessageHandler():
	def parse_message(message):
		# sample message: client:' + 'createfile:' + dfs_path + ':' + size
		parsed = message.split(':')
		sender_type = parsed[0]
		command = parsed[1]
		args = parsed[2:]

		if sender_type not in ['client', 'chunk']:
			return 'Invalid sender type'
		return sender_type, command, args		


class MasterServer():
	def __init__(self, host, port):
		self.host = host
		self.port = port
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR)
		self.sock.bind((self.host, self.port))
		self.NUM_CHUNKS = config.NUM_CHUNKS
		self.chunk_map = {}
		self.files = {}


	def listen(self):
		self.sock.listen(5)
		while True:
			client, address = self.sock.accept()
			client.timeout(60)
			handler = threading.Thread(target=self.service, args=(client, address))
			handler.start()

	def service(self, client, address):
		# listen here from client or chunk server
		pass

	def create_file(self, dfs_path, size):
		'''Check if file already exists, if not create a new file with the given number of chunks and return the chunk locations'''
		if dfs_path in self.files:
			return 'File already exists'
		

	def get_chunk_locs(self, num_chunks, dfs_path):
		
		chunk_locs = self.__get_chunk_locs(num_chunks)
		self.files[dfs_path] = chunk_locs
		return chunk_locs

	def __get_chunk_locs(self, num_chunks):
		# return a chunk id and a list of chunk server locatons based on replication factor
		locs = []
		for chunk in num_chunks:
			chunk_id = self.__assign_chunk_id()
			chunk_servers = self.__assign_chunk_server()
			locs.append((chunk_id, chunk_servers))
			self.chunk_map[chunk_id] = chunk_servers
		return locs
	
	def __assign_chunk_server(replication_factor = config.REPLICATION_FACTOR):
		return random.choices(config.CHUNK_PORTS, replication_factor)

	def __assign_chunk_id():
		return str(uuid.uuid4())
	




if __name__ == '__main__':
	print('Master Server Running')
	ms = MasterServer(config.HOST, config.MASTER_PORT)
	ms.listen()
