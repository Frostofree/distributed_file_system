import socket
import threading

import config
import uuid as uuid
import random

import pickle


class Directory():
	def __init__(self, dfs_path: str):
		self.dfs_path = dfs_path
		self.files = {}
		self.subdirectories = {}
	def add_dir(self, name: str):
		if name in self.subdirectories:
			print( f'Directory {name} already exists')
		self.children[name] = Directory(name)

	def add_file(self, name: str, size: int):
		if name in self.files:
			print( f'File {name} already exists')
		self.children[name] = File(name, size)
class File():
	def __init__(self, dfs_path: str):
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
		self.root = Directory('/')
		

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
				self.get_chunk_locs(client, args[0], args[1])
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
		print("file created")
		return 'File created'
		
	
	def list_files(self, client, dfs_path):
		print("in ls function")
		'''List all files in the system'''
		message = list(self.files.keys())
		client.send(pickle.dumps(message))
		print("files listed")

	def get_chunk_locs(self, client, dfs_path, current_chunk): 
		chunk_handle = self.__assign_chunk_handle()
		chunk_servers = self.__assign_chunk_server()
		self.chunk_map[chunk_handle] = chunk_servers
		file = self.files[dfs_path]
		file.chunks[current_chunk] = (chunk_handle, chunk_servers)
		response_data = {
        	"chunk_handle": chunk_handle,
        	"chunk_servers": chunk_servers
    	}
		serialized_data = pickle.dumps(response_data)
		client.send(serialized_data)
	
	
	def __assign_chunk_server(replication_factor = config.REPLICATION_FACTOR):
		return random.choices(config.CHUNK_PORTS, replication_factor)

	def __assign_chunk_handle():
		return str(uuid.uuid4())
	


if __name__ == '__main__':
	print('Master Server Running')
	ms = MasterServer(config.HOST, config.MASTER_PORT)
	ms.listen()
