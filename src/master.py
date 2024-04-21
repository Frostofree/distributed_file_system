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

from collections import OrderedDict, defaultdict
import time


chunk_to_chunk_ids = defaultdict(lambda: []) # dictionary of lists
chunk_ids_to_chunks = defaultdict(lambda: []) # dictionary of lists
chunk_ids_to_replicate = defaultdict(lambda: []) # dictionary of lists
alive_chunk_servers_list = [] ### initialize
alive_chunk_servers = 0
lock_dict = defaultdict(lambda: None)
lock = threading.Lock()
dead = defaultdict(lambda: False)


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
class FileStatus():
	CREATING = 0
	COMMITTED = 1
	ABORTED = 2
	DELETED = 3

class File():
	def __init__(self, name, dfs_path: str):
		self.name = name
		self.dfs_path = dfs_path
		self.size = 0
		self.chunks = {}
		self.status = None
		self.is_locked = False
	

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
		self.locks = {} #clientip+port to file object
		self.connected_clients = [] # list of currently connected clients
		
		

	def listen(self):
		self.sock.listen()
		while True:
			client, address = self.sock.accept()
			handler = threading.Thread(target=self.service, args=(client, address))
			handler.start()

	def service(self, client, address):
		ip, port = address
		self.connected_clients.append((ip, port))
		connected = True
		current_command = None
		current_args = None
		while connected:
			try:
				current_command = None
				current_args = None
				message = client.recv(config.MESSAGE_SIZE)
				if message == b'':
					raise socket.error
				if message:
					message = json.loads(message.decode('utf-8'))
					sender_type = message['sender_type']
					command = message['function']
					args = message['args']

					current_command = command
					current_args = args

				if sender_type == 'client':
					if command == 'create_file':
						self.create_file(client, args)
					elif command == 'set_chunk_loc':
						self.set_chunk_loc(client, args)
					elif command == 'commit_file':
						self.commit_file(client, args)
					elif command == 'commit_delete':
						self.commit_delete(client, args)
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
				connected = False 
				self.connected_clients.remove((ip, port))
				# check if the client was holding any file locks 
			

				to_pop = None
				for key, file in self.locks.items():
					lock_ip, lock_port = key.split(':')
					str_port = str(port)	
					print(type(lock_ip), type(ip), type(lock_port), type(port))
					if lock_ip == ip and lock_port == str_port:
						to_pop = key						
						file.status = FileStatus.ABORTED
						print(f"File {file.name} lock released")
						print(f"Aborted file {file.name}")
				self.locks.pop(to_pop)
				client.close()
	

	def heart_beat_handler(self):
		global alive_chunk_servers
		global lock
		global lock_dict
		global dead
		while True:
			time.sleep(config.HEART_BEAT_INTERVAL)
			for idx, port in enumerate(config.CHUNK_PORTS):	
				try:
					sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					sock.settimeout(20)
					sock.connect((self.host, port))
					request = self.__respond_message("heartbeat", [])
					sock.send(request)
					response = sock.recv(config.MESSAGE_SIZE)
					response = json.loads(response.decode('utf-8'))
					if response['status'] == 0:
						# print("Active!")
						with lock:
							if(port not in alive_chunk_servers_list):
								alive_chunk_servers_list.append(port)
								alive_chunk_servers += 1
								lock_dict[port] = threading.Lock()
								dead[port] = False
					else:
						raise Exception
				except socket.error as e:
					print(f"Chunk Server with IP {self.host} and Port {port} not responding.")
					sock.close()
					if(not dead[port]):
						with lock:
							dead[port] = True
						self.check_chunk_server(port)
				except Exception as e:
					print(e)
					print(f"Chunk Server with IP {self.host} and Port {port} not up.")
					sock.close()
					if(not dead[port]):
						with lock:
							dead[port] = True
						self.check_chunk_server(port)
			curr_dir = self.root
			self.purge_chunks(curr_dir)

	def purge_chunks(self, dir):
		for file_name in dir.files:
			file = dir.files[file_name]
			if not file.is_locked:
				if file.status in [FileStatus.CREATING, FileStatus.DELETED, FileStatus.ABORTED]:
					for chunk_id, chunk_locs in file.chunks.items():
						for chunk_loc in chunk_locs:
							try:
								sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
								sock.connect((self.host, config.CHUNK_PORTS[chunk_loc]))
								request = self._get_message_data('delete_chunk', chunk_id)
								sock.sendall(request)
								response = sock.recv(config.MESSAGE_SIZE)
								response = json.loads(response.decode('utf-8'))
								if response['status'] == -1:
									pass
								sock.close()
							except Exception as e:
								pass


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
			return
		else:
			file = curr_dir.files[file_name]
			if file.status != FileStatus.COMMITTED:
				client.send(self.__respond_status(-1, "File is not committed, will be purged"))
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
	def commit_delete(self, client, args):
		dir = args[0]
		file = args[1]
		directory = [part for part in dir.split('/') if part]
		curr_dir = self.root
		for d in directory:
			if d not in curr_dir.subdirectories:
				client.send(self.__respond_status(-1, 'Directory does not exist'))
				return
			curr_dir = curr_dir.subdirectories[d]
		if file not in curr_dir.files.keys():
			client.send(self.__respond_status(-1, 'File does not exist'))
			return
		else:
			curr_dir.files.pop(file)
			client.send(self.__respond_status(0, 'Done'))
		print(f"Deleted file {file}")
		
		


	def list_files(self, client, args):
		dfs_dir = args[0]

		directory = [part for part in dfs_dir.split('/') if part]
		curr_dir = self.root

		for d in directory: 
			if d not in curr_dir.subdirectories:
				client.send(self.__respond_status(-1, 'Directory does not exist'))
				return
			curr_dir = curr_dir.subdirectories[d]

		data = list(curr_dir.files.keys())
		# remove the files that are not committed
		data = [file for file in data if curr_dir.files[file].status == FileStatus.COMMITTED]
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
			file.status = FileStatus.DELETED
			curr_dir.files.pop(file_name)#
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
			file = curr_dir.files[file_name]
			file.status = FileStatus.CREATING
			ip, port = client.getpeername()
			lock_key = str(ip) + ":" + str(port)
			self.locks[lock_key] = file
			file.is_locked = True
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

		with lock:
			for chunk_loc in chunk_locs:
				chunk_to_chunk_ids[config.CHUNK_PORTS[chunk_loc]].append(chunk_id)
			for chunk_loc in chunk_locs:
				chunk_ids_to_chunks[chunk_id].append(config.CHUNK_PORTS[chunk_loc])

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
	def commit_file(self, client, args):
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
		else:
			file = curr_dir.files[dfs_name]
			file.status = FileStatus.COMMITTED
			lock_key = str(client.getpeername()[0]) + ":" + str(client.getpeername()[1])
			file = self.locks[lock_key]
			self.locks.pop(lock_key)
			file.is_locked = False
			print(f"commited file {dfs_name} ")

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
	
	def isdead(self, chunk_server):
		global dead
		p = 0
		with lock:
			if(dead[chunk_server]):
				p = 1
		return p
			

	def _get_message_data(self, function, *args):
		function = function
		message = {
			'sender_type': 'master',
			'function': function,
			'args': args
		}

		# encode the message in utf8
		encoded = json.dumps(message).encode('utf-8')
		encoded += b' ' * (config.MESSAGE_SIZE - len(encoded))
		return encoded

	def send_replicate_message(self, chunk_loc, chunk_id, new_chunk_loc):
		# ask chunk_loc to replicate chunk_id on to new_chunk_loc
		# add timeout and return 0 if it times out
		try:
			chunk_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			chunk_server.connect((socket.gethostbyname('localhost'), chunk_loc)) # chunk_loc here is the port number or some unique id
			# chunk_server.settimeout(1)
			request = self._get_message_data('replicate_chunk', {"chunk_id": chunk_id, "new_chunk_loc": new_chunk_loc})	
			chunk_server.sendall(request)
			response = chunk_server.recv(config.MESSAGE_SIZE)
			response = json.loads(response.decode('utf-8'))
			# print("Chunk with chunk id: ", chunk_id, " replicated successfully")
			return response['status']
		except socket.timeout:
			print("Socket operation timed out on commanding chunk server to replicate chunk id: ", chunk_id)
			return 0
		except socket.error:
			print("Socket error in function send_replicate_message of master")
		except:
			print("An error occurred while commanding chunk server to replicate chunk id: ", chunk_id)
			return 0
			
	def isspecialcase(self, chunk_id):
		global alive_chunk_servers_list
		global chunk_to_chunk_ids
		global lock
		# consider lock? 
		p = -1
		with lock:
			for chunk_server in alive_chunk_servers_list:
				if(chunk_id not in chunk_to_chunk_ids[chunk_server]):
					p = 0
					break
		if(p == 0):
			return 0
		return 1

	# chunk_loc is the chunk server from where it will be replicated
	def replicate(self, chunk_loc, chunk_id):
		global alive_chunk_servers
		global chunk_to_chunk_ids
		global lock
		if(alive_chunk_servers <= 1):
			return 1 # cant do better
		# while not replicated
		while(True):
			# Handle case where only those chunk servers are active which already contain the chunk
			if(self.isspecialcase(chunk_id)):
				return 1 # cant do better
			print("Not special case")
			new_chunk_loc = chunk_loc
			print("original chunk_loc: ", chunk_loc)
			while(chunk_id in chunk_to_chunk_ids[new_chunk_loc] or self.isdead(new_chunk_loc) or chunk_loc == new_chunk_loc): ### new_chunk loc is not dead condition
				new_chunk_loc = random.sample(config.CHUNK_PORTS, 1)[0]
				print("Trying chunk loc: ", new_chunk_loc)
			print("New chunk_loc: ", new_chunk_loc)
			status = self.send_replicate_message(chunk_loc, chunk_id, new_chunk_loc)
			# time.sleep(5)
			if(status == 1):
				break
			else:
				print("Still trying")
		# error handling already in main function check_chunk_server
		return status

	def check_chunk_server(self, chunk_server):
		### make all variables global
		global lock_dict
		global chunk_ids_to_replicate
		global alive_chunk_servers_list
		global alive_chunk_servers
		global chunk_to_chunk_ids
		global chunk_ids_to_chunks
		count = 0
		if(lock_dict[chunk_server] is None):
			return # chunk_server does not exist so no need to check
		# if(self.isdead(chunk_server)):
		print("Chunk server on port: ", chunk_server, " died")
		try:
			lock_dict[chunk_server].acquire()
			print("Got lock")
			### repeated check
			if(chunk_server in alive_chunk_servers_list):
				alive_chunk_servers -= 1
				alive_chunk_servers_list.remove(chunk_server)
			else:
				print("Releasing lock at line 470")
				lock_dict[chunk_server].release()
				return
			with lock:
				chunk_ids_to_replicate[chunk_server] = []
				chunk_ids = chunk_to_chunk_ids[chunk_server]
				chunk_ids_to_replicate[chunk_server] = chunk_ids
				count = len(chunk_ids_to_replicate[chunk_server])
			if(count == 0):
				print("Nothing to replicate")
			print("Chunk loc with the dead chunk id: ", chunk_ids_to_chunks[chunk_to_chunk_ids[chunk_server][0]][0])
			for chunk_id in chunk_ids_to_replicate[chunk_server]:
				for chunk_loc in chunk_ids_to_chunks[chunk_id]:
					if(self.isdead(chunk_loc)): # itself needs replication and so is down
						continue
					print("Trying to replicate")
					lock_dict[chunk_loc].acquire()
					try:
						status = self.replicate(chunk_loc, chunk_id)
						if(status == 1):
							count -= 1
							continue
						else:
							print("Could not replicate chunk id: ", chunk_id)
					except:
						print("Error during replication of chunk id: ", chunk_id)
					finally:
						lock_dict[chunk_loc].release()
			if(count > 0):
				print("Could not replicate all chunks of chunk server: ", chunk_server)
			# resetting the parameters to new correct configuration
			chunk_ids_to_replicate[chunk_server] = []
			for chunk_id in chunk_to_chunk_ids[chunk_server]:
				chunk_ids_to_chunks[chunk_id].remove(chunk_server)
			chunk_to_chunk_ids[chunk_server] = []
			print("Releasing lock at line 500")
			lock_dict[chunk_server].release()
		except:
			print("Error in checking dead chunk servers")
		# else: # if alive there is no need for replication, can simply return
		# 	return


if __name__ == '__main__':
	print('Master Server Running')
	ms = MasterServer(config.HOST, config.MASTER_PORT)
	ms.listen()