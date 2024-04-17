import socket
import threading

import config
import uuid as uuid
import random

import pickle


class File():
	def _init_(self, dfs_path: str):
		self.dfs_path = dfs_path
		self.size = None
		self.chunks = {}
		self.status = None

	def __repr__(self):
		return f"Path: {self.dfs_path}, Size: {self.size}, Status: {self.status}" 


def parse_message(message):
		# sample message: client:' + 'createfile:' + dfs_path + ':' + size
		parsed = message.split(':')
		sender_type = parsed[0]
		command = parsed[1]
		args = parsed[2:]
		return sender_type, command, args


class MasterServer():
	def __init__(self, host, port):
		self.host = host
		self.port = port
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.sock.bind((self.host, self.port))
		self.NUM_CHUNKS = config.NUM_CHUNKS
		self.files = {} # maps file path to file object


	def listen(self):
		self.sock.listen(5)
		while True:
			client, address = self.sock.accept()
			client.settimeout(60)
			handler = threading.Thread(target=self.service, args=(client, address))
			handler.start()

	def service(self, client, address):
		sender_type, command, args = parse_message(client.recv(1024).decode('utf-8'))
		if sender_type == 'client':
			if command == 'create_file':
				self.create_file(args[0])
			elif command == 'get_chunk_locs':
				self.get_chunk_locs(args[0], args[1])
			elif command == 'commit_chunk':
				self.commit_chunk(args[0], args[1])
			elif command == 'file_create_status':
				self.file_create_status(args[0], int(args[1]))
			elif command == 'get_chunk_details':
				self.get_chunk_details(args[0], int(args[1]))
			elif command == 'list_files':
				self.list_files(client, args[0])
			elif command == 'delete_file':
				self.delete_file(args[0], int(args[1]))
				pass
		elif sender_type == 'chunkserver':
			pass
		else:
			print('Invalid Sender Type!')


	def create_file(self, dfs_path):
		'''Check if file already exists, if not create a new file with the given number of chunks and return the chunk locations'''
		if dfs_path in self.files:
			return 'File already exists'
		file = File(dfs_path) 
		self.files[dfs_path] = file
		return 'File created'
		print("file created")
	
	def list_files(self, client, dfs_path):
		'''List all files in the system'''
		message = self.files.keys()
		client.send(pickle.dumps(message))
		print("files listed")

	def get_chunk_locs(self, num_chunks, dfs_path, current_chunk): 
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
