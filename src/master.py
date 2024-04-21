import socket
import threading
import logging

import config
import uuid as uuid
import random
import os.path

import sys
import json
import pickle

from collections import OrderedDict
import time


class Logger():
	def __init__(self, log_file):
		logging.basicConfig(filename=log_file, format='%(message)s')
		self.log = logging.getLogger('Master')
		self.log.setLevel(logging.INFO)
		self.root = Directory('/')
		self.restore(log_file)

	def restore(self, log_file):
		if os.path.exists(log_file):
			with open(log_file, 'r') as f:
				while True:
					line = f.readline().strip()
					if len(line) == 0:
						break
					line = line.split()
					command = line[0]
					if command == 'create':
						directory, file_name = line[1], line[2]
						directory = [part for part in directory.split('/') if part]
						curr_dir = self.root
						for d in directory: 
							curr_dir = curr_dir.subdirectories[d]
						curr_dir.add_file(file_name)
					elif command == 'create_dir':
						dir_loc, new_dir = line[1], line[2]
						print(dir_loc, new_dir)
						dir_loc = [part for part in dir_loc.split('/') if part]
						curr_dir = self.root
						for d in dir_loc:
							curr_dir = curr_dir.subdirectories[d]
						curr_dir.subdirectories[new_dir] = Directory(curr_dir.dfs_path + '/' + new_dir)
					elif command == 'set_chunk_loc':
						dfs_dir, dfs_name = line[1], line[2]
						directory = [part for part in dfs_dir.split('/') if part]
						curr_dir = self.root
						for d in directory: 
							curr_dir = curr_dir.subdirectories[d]
						chunk_id, chunk_locs = line[3], line[4]
						file = curr_dir.files[dfs_name]
						file.chunks[chunk_id] = chunk_locs
					elif command == 'delete':
						directory, file_name = line[1], line[2]
						directory = [part for part in directory.split('/') if part]
						curr_dir = self.root
						for d in directory: 
							curr_dir = curr_dir.subdirectories[d]
						file = curr_dir.files[file_name]
						curr_dir.files.pop(file_name)



	def log_info(self, command, args):
		if command == 'create':
			self.log.info(f'create {args[0]} {args[1]}')
		elif command == 'create_dir':
			self.log.info(f'create_dir {args[0]} {args[1]}')
		elif command == 'set_chunk_loc':
			self.log.info(f'set_chunk_loc {args[0]} {args[1]} {args[2]} {args[3]}')
		elif command == 'delete':
			self.log.info(f'delete {args[0]} {args[1]}')



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
		self.chunks = {}
		self.status = None

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
		self.logger = Logger(config.MASTER_LOG)
		self.root = self.logger.root
		heart_beat_thread = threading.Thread(target=self.heart_beat_handler)
		heart_beat_thread.start()

		

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

	def heart_beat_handler(self):
		while True:
			time.sleep(config.HEART_BEAT_INTERVAL)
			print("pinging servers")
			for port in config.CHUNK_PORTS:
				try:
					sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					sock.settimeout(1)
					sock.connect((self.host, port))
					request = self.__respond_message("heartbeat", [])
					sock.send(request)
					response = sock.recv(config.MESSAGE_SIZE)
					response = json.loads(response.decode('utf-8'))
					if response['status'] == 0:
						print(f"Chunk Server with IP {self.host} and Port {port} is up.")
					else:
						raise Exception
				except socket.error as e:
					print(f"Chunk Server with IP {self.host} and Port {port} not responding.")
					sock.close()
				except Exception as e:
					print(e)
					print(f"Chunk Server with IP {self.host} and Port {port} not up.")
					sock.close()


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

			for id, loc in file.chunks.items():
				response = {
					'status': 0,
					'chunk_id': id,
					'chunk_loc': loc
				}
				response = json.dumps(response).encode('utf-8')
				response += b' ' * (config.MESSAGE_SIZE - len(response))
				client.send(response)

			client.send(self.__respond_status(1, 'Done'))



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

			for id, loc in file.chunks.items():
				
				response = {
					'status': 0,
					'chunk_id': id,
					'chunk_loc': loc
				}
				response = json.dumps(response).encode('utf-8')
				response += b' ' * (config.MESSAGE_SIZE - len(response))
				client.send(response)

			self.logger.log_info('delete', [args[0], args[1]])
			client.send(self.__respond_status(1, 'Done'))

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
			self.logger.log_info('create', [args[0], args[1]])
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

		file.chunks[chunk_id] = chunk_locs

		response = {
			'chunk_id': chunk_id,
			'chunk_locs': chunk_locs
		}

		self.logger.log_info('set_chunk_loc', [args[0], args[1], chunk_id, chunk_locs])
		response = json.dumps(response).encode('utf-8')
		response += b' ' * (config.MESSAGE_SIZE - len(response))
		client.send(response)

	def create_dir(self, client, args):
		dir_loc = args[0]
		new_dir = args[1]
		print(dir_loc, new_dir)

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
			self.logger.log_info('create_dir', [args[0], args[1]])
			curr_dir.subdirectories[new_dir] = Directory(curr_dir.dfs_path + '/' + new_dir)
			client.send(self.__respond_status(0, 'Directory Created'))


	def close_connection(self, client):
		client.close()

	
	def _create_chunk_id(self):
		return str(uuid.uuid4())
	
	def _sample_chunk_locs(self):
		chunk_locs = random.sample(range(0, self.NUM_CHUNKS), config.REPLICATION_FACTOR)
		return chunk_locs
		

	def __respond_message(self, function, args):
		message = {
			'sender_type': 'master',
			'function': function,
			'args': args
		}
		message = json.dumps(message).encode('utf-8')
		message += b' ' * (config.MESSAGE_SIZE - len(message))
		return message

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