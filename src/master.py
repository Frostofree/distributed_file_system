import socket
import threading

import config
import uuid as uuid
import random

import json
import pickle

from collections import OrderedDict


class Directory():
	def __init__(self, dfs_path: str):
		self.dfs_path = dfs_path
		self.files = {}
		self.subdirectories = {}

	def add_file(self, name: str):
		path = self.dfs_path + '/' + name
		self.files[name] = File(name, path)

class File():
	def __init__(self, name, dfs_path: str):
		self.dfs_path = dfs_path
		self.size = 0
		self.chunks = {} # maps chunk id to the servers
		self.chunk_ordering = OrderedDict()
		self.status = None

		# chunk_server1: 1 2 4 5 8
		# chunk_server2: 1 3 4 6 7 8
		# chunk_server3: 2 3 6 7 5
  
		# self.chunks
		# 1: cs1 cs2
		# 2: cs1 cs3

	def __repr__(self):
		return f"Path: {self.dfs_path}, Size: {self.size}, Status: {self.status}" 

class MasterServer():
	def __init__(self, host, port):
		self.host = host
		self.port = port
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.sock.bind((self.host, self.port))
		self.NUM_CHUNKS = config.NUM_CHUNKS
		self.root = Directory('/')
		

	def listen(self):
		self.sock.listen()
		while True:
			client, address = self.sock.accept()
			handler = threading.Thread(target=self.service, args=(client, address))
			handler.start()

	def service(self, client, address):
		ip, port = address
		connected = True
		while connected:
			try:
				message = client.recv(config.MESSAGE_SIZE)
				if message == b'':
					raise socket.error
				if message:
					message = json.loads(message.decode('utf-8'))
					sender_type = message['sender_type']
					command = message['function']
					args = message['args']

				if sender_type == 'client':
					if command == 'create_file':
						self.create_file(client, args)
					elif command == 'set_chunk_loc':
						self.set_chunk_loc(client, args)
					elif command == 'create_dir':
						self.create_dir(client, args)
					elif command == 'read_file':
						self.read_file(client, args)
					elif command == 'list_files':
						self.list_files(client, args)
					elif command == 'delete_file':
						self.delete_file(client, args)
					elif command == 'close':
						self.close_connection(client)
						connected = False
						print(f"Client with IP {ip} and Port {port} disconnected.")
						
			except socket.error:
				print(f"Client with IP {ip} and Port {port} unexpectedly disconnected.")
				client.close()
				connected = False 


	def read_file(self, client, args):
		directory = args[0]
		file_name = args[1]

		directory = [part for part in directory.split('/') if part]
		curr_dir = self.root

		for d in directory: 
			if d not in curr_dir.subdirectories:
				client.send(self.__respond_status(-1, 'Directory does not exist'))
				return
			curr_dir = curr_dir.subdirectories[d]

		if file_name not in curr_dir.files:
			client.send(self.__respond_status(-1, 'File does not exist'))
		else:
			file = curr_dir.files[file_name]

			chunk_ids, chunk_locs = [], []
			for id, loc in file.chunks.items():
				chunk_ids.append(id)
				chunk_locs.append(loc)

			response = {
				'status': 0,
				'chunk_ids': chunk_ids,
				'chunk_locs': chunk_locs
			}

			response = json.dumps(response).encode('utf-8')
			response += b' ' * (config.MESSAGE_SIZE - len(response))
			client.send(response)



	def list_files(self, client, args):
		dfs_dir = args[0]
		# print(dfs_dir)

		directory = [part for part in dfs_dir.split('/') if part]
		curr_dir = self.root
		# print(directory, curr_dir)

		for d in directory: 
			if d not in curr_dir.subdirectories:
				client.send(self.__respond_status(-1, 'Directory does not exist'))
				return
			curr_dir = curr_dir.subdirectories[d]

		# print(curr_dir, curr_dir.files)
		data = list(curr_dir.files.keys())
		# print(data)
		response = {
			'status': 0,
			'data': data
		}

		response = json.dumps(response).encode('utf-8')
		response += b' ' * (config.MESSAGE_SIZE - len(response))
		client.send(response)



	def delete_file(self, client, args):
		directory = args[0]
		file_name = args[1]

		directory = [part for part in directory.split('/') if part]
		curr_dir = self.root

		for d in directory: 
			if d not in curr_dir.subdirectories:
				client.send(self.__respond_status(-1, 'Directory does not exist'))
				return
			curr_dir = curr_dir.subdirectories[d]

		if file_name not in curr_dir.files:
			client.send(self.__respond_status(-1, 'File does not exist'))
		else:
			file = curr_dir.files[file_name]
			curr_dir.files.pop(file_name)

			chunk_ids, chunk_locs = [], []
			for id, loc in file.chunks.items():
				chunk_ids.append(id)
				chunk_locs.append(loc)

			response = {
				'status': 0,
				'chunk_ids': chunk_ids,
				'chunk_locs': chunk_locs
			}

			response = json.dumps(response).encode('utf-8')
			response += b' ' * (config.MESSAGE_SIZE - len(response))
			client.send(response)

	
				

	def create_file(self, client, args):
		directory = args[0]
		file_name = args[1]

		directory = [part for part in directory.split('/') if part]
		curr_dir = self.root
		
		for d in directory: 
			if d not in curr_dir.subdirectories:
				client.send(self.__respond_status(-1, 'Directory does not exist'))
				return
			curr_dir = curr_dir.subdirectories[d]

		if file_name in curr_dir.files:
			client.send(self.__respond_status(-1, 'File already exists'))
		else:
			curr_dir.add_file(file_name)
			print(f"added file {file_name}")
			client.send(self.__respond_status(0, 'File Created'))



	def set_chunk_loc(self, client, args):
		dfs_dir = args[0]
		dfs_name = args[1]

		directory = [part for part in dfs_dir.split('/') if part]
		curr_dir = self.root
		
		for d in directory: 
			if d not in curr_dir.subdirectories:
				client.send(self.__respond_status(-1, 'Directory does not exist'))
				return
			curr_dir = curr_dir.subdirectories[d]

		if dfs_name not in curr_dir.files:
			client.send(self.__respond_status(-1, 'File does not exist'))
		
		chunk_id = self._create_chunk_id()
		chunk_locs = self._sample_chunk_locs()

		file = curr_dir.files[dfs_name]
		if not file.chunk_ordering:
			file.chunk_ordering[0] = chunk_id
		else:
			# last_key, last_chunk_id = file.chunk_ordering.popitem(last=True)
			last_key = len(file.chunk_ordering)
			file.chunk_ordering[last_key + 1] = chunk_id

		file.chunks[chunk_id] = chunk_locs

		response = {
			'chunk_id': chunk_id,
			'chunk_locs': chunk_locs
		}

		response = json.dumps(response).encode('utf-8')
		response += b' ' * (config.MESSAGE_SIZE - len(response))
		client.send(response)

	def create_dir(self, client, args):
		print(args)
		dir_loc = args[0]
		new_dir = args[1]

		dir_loc = [part for part in dir_loc.split('/') if part]
		curr_dir = self.root
		
		for d in dir_loc:
			if d not in curr_dir.subdirectories:
				client.send(self.__respond_status(-1, 'Directory does not exist'))
				return
			curr_dir = curr_dir.subdirectories[d]

		if new_dir in curr_dir.subdirectories:
			client.send(self.__respond_status(-1, 'Directory already exists'))
		else:
			curr_dir.subdirectories[new_dir] = Directory(curr_dir.dfs_path + '/' + new_dir)
			client.send(self.__respond_status(0, 'Directory Created'))

	def close_connection(self, client):
		client.close()

	
	def _create_chunk_id(self):
		return str(uuid.uuid4())
	
	def _sample_chunk_locs(self):
		chunk_locs = random.sample(range(0, self.NUM_CHUNKS), config.REPLICATION_FACTOR)
		return chunk_locs
		


	def __respond_status(self, code, message):
		response =  {
			'status': code,
			'message': message
		}
		response = json.dumps(response).encode('utf-8')
		response += b' ' * (config.MESSAGE_SIZE - len(response))
		return response
	
	


if __name__ == '__main__':
	print('Master Server Running')
	ms = MasterServer(config.HOST, config.MASTER_PORT)
	ms.listen()
