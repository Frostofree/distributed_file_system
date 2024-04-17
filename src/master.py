import socket
import threading

import config
import uuid as uuid
import random


class File():
	def _init_(self, dfs_path: str):
		self.dfs_path = dfs_path
		self.size = None
		self.chunks = {}
		self.status = None
		
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
	def _init_(self, host, port):
		self.host = host
		self.port = port
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR)
		self.sock.bind((self.host, self.port))
		self.NUM_CHUNKS = config.NUM_CHUNKS
		self.files = {} # maps file path to file object


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
		file = File(dfs_path) 
		self.files[dfs_path] = file
		return 'File created'
	

	def get_chunk_locs(self, num_chunks, dfs_path, current_chunk): 
		# has to send back a uid and a list of chunk locations
		chunk_handle = self.__assign_chunk_handle()
		chunk_servers = self.__assign_chunk_server()
		self.chunk_map[chunk_handle] = chunk_servers
		file = self.files[dfs_path]
		file.chunks[current_chunk] = (chunk_handle, chunk_servers)
		return (chunk_handle, chunk_servers)
	
	def __assign_chunk_server(replication_factor = config.REPLICATION_FACTOR):
		return random.choices(config.CHUNK_PORTS, replication_factor)

	def __assign_chunk_handle():
		return str(uuid.uuid4())
	


if __name__ == '__main__':
	print('Master Server Running')
	ms = MasterServer(config.HOST, config.MASTER_PORT)
	ms.listen()
