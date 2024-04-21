import socket
import pickle
import os.path
import threading
import time
import errno

import config
import json



class Client():
	def __init__(self):
		self.master_dead = True


	def create_dir(self, dfs_dir, new_dir):
		try:
			request = self._get_message_data('create_dir', dfs_dir, new_dir)
			self.master.sendall(request)
			response = self.master.recv(config.MESSAGE_SIZE)
			if not response:
				raise Exception('Master not responding!')
			response = json.loads(response.decode('utf-8'))
			print(response['message'])
		except Exception as e:
			self.master.close()
			self.master_dead = True
			if "[Errno 32] Broken pipe" in str(e):
				print('Master not responding!')
			else: print(e)

	def create_file(self, local_path, dfs_dir, dfs_name):
		try:
			if not os.path.exists(local_path):
				raise Exception('Local file does not exist')
			request = self._get_message_data('create_file', dfs_dir, dfs_name)
			self.master.sendall(request)
			response = self.master.recv(config.MESSAGE_SIZE)
			if not response:
				raise Exception('Master not responding!')
			response = json.loads(response.decode('utf-8'))
			if response['status'] == -1:
				print(response['message'])
				return

			num_bytes = os.path.getsize(local_path)
			chunks = num_bytes // config.CHUNK_SIZE + int(num_bytes%config.CHUNK_SIZE != 0)

			dead_chunks = [0]*config.NUM_CHUNKS
			for chunk in range(chunks):
				request = self._get_message_data('set_chunk_loc', dfs_dir, dfs_name)
				self.master.send(request)
				response = self.master.recv(config.MESSAGE_SIZE)
				if not response:
					raise Exception('Master not responding!')
				response = json.loads(response.decode('utf-8'))
				chunk_id = response['chunk_id']
				chunk_locs = response['chunk_locs']

				with open(local_path, 'rb') as f:
					f.seek(chunk * config.CHUNK_SIZE)
					data = f.read(config.CHUNK_SIZE)

				count = 0
				for chunk_loc in chunk_locs:
					try:
						chunk_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
						chunk_server.connect((socket.gethostbyname('localhost'), config.CHUNK_PORTS[chunk_loc]))
						request = self._get_message_data('write_chunk', chunk_id)
						chunk_server.sendall(request)
						chunk_server.sendall(data)
						chunk_server.close()
						dead_chunks[chunk_loc] = 0
						count += 1
					except Exception as e:
						
						if "[Errno 111] Connection refused" in str(e):
							dead_chunks[chunk_loc] = 1
						
					
				if sum(dead_chunks) == config.NUM_CHUNKS:
					request = self._get_message_data('file_failed', dfs_dir, dfs_name)
					self.master.send(request)

				if count == 0:
					print('Could not create file. Try again...')
					return

			request = self._get_message_data("commit_file", dfs_dir, dfs_name)
			self.master.send(request)
		except Exception as e:
			self.master.close()
			self.master_dead = True
			if "[Errno 32] Broken pipe" in str(e):
				print('Master not responding!')
			else: print(e)


	def read_file(self, dfs_dir, dfs_name):
		fail = False

		try:
			request = self._get_message_data('read_file', dfs_dir, dfs_name)
			self.master.sendall(request)
			chunk_ids, chunks_locs = [], []
			while True:
				response = self.master.recv(config.MESSAGE_SIZE)
				if not response:
					raise Exception('Master not responding!')
				response = json.loads(response.decode('utf-8'))
				if response['status'] == -1:
					print(response['message'])
					fail = True
					break
				if response['status'] == 1:
					break
				chunk_ids.append(response['chunk_id'])
				chunks_locs.append(response['chunk_loc'])

			if not fail:
				data = ''
				final_success = True
				for id, locs in zip(chunk_ids, chunks_locs):
					success = False
					for loc in locs:
						if success == False:
							try:
								chunk_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
								chunk_server.connect((socket.gethostbyname('localhost'), config.CHUNK_PORTS[loc]))
								request = self._get_message_data('read_chunk', id)	
								chunk_server.sendall(request)
								response = chunk_server.recv(config.MESSAGE_SIZE)
								response = json.loads(response.decode('utf-8'))
								if response['status'] == -1:
									success = False
								else:
									data += response['data']
									success = True
							except:
								success = False
						else:
							break
					if success == False:
						final_success = False
						break
				
				if final_success == False:
					print("Can't read file now. Try again later")
					self.master.send(self._get_status_data(-1, "Can't read file now. Try again later"))
				else:
					self.master.send(self._get_status_data(0, "ok"))
					print(data)
		except Exception as e:
			self.master.close()
			self.master_dead = True
			if "[Errno 32] Broken pipe" in str(e):
				print('Master not responding!')
			# else: print(e)
		
	
	def list_files(self, dfs_dir):
		try:
			request = self._get_message_data('list_files', dfs_dir)
			self.master.sendall(request)
			response = self.master.recv(config.MESSAGE_SIZE)
			if not response:
				raise Exception('Master not responding!')
			response = json.loads(response.decode('utf-8'))
			if response['status'] == -1:
				print(response['message'])
				return
			for file in response['data']:
				print(file)
			for directory in response['directories']:
				print(directory+'/')
		except Exception as e:
			self.master.close()
			self.master_dead = True
			if "[Errno 32] Broken pipe" in str(e):
				print('Master not responding!')
			else: print(e)


	def delete_file(self, dfs_dir, dfs_name):
		fail = False
		try:
			request = self._get_message_data('delete_file', dfs_dir, dfs_name)
			self.master.sendall(request)
			chunk_ids, chunks_locs = [], []
			while True:
				# time.sleep(0.2)
				response = self.master.recv(config.MESSAGE_SIZE)
				if not response:
					raise Exception('Master not responding!')
				response = json.loads(response.decode('utf-8'))
				if response['status'] == -1:
					print(response['message'])
					fail = True
					break
				if response['status'] == 1:
					break
				chunk_ids.append(response['chunk_id'])
				chunks_locs.append(response['chunk_loc'])

			if not fail:
				for id, locs in zip(chunk_ids, chunks_locs):
					dead_chunks = [0]*config.NUM_CHUNKS
					for loc in locs:
						try:
							chunk_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
							chunk_server.connect((socket.gethostbyname('localhost'), config.CHUNK_PORTS[loc]))
							request = self._get_message_data('delete_chunk', id)	
							chunk_server.sendall(request)
							response = chunk_server.recv(config.MESSAGE_SIZE)
							if not response:
								raise Exception('Chunkserver not responding!')
							response = json.loads(response.decode('utf-8'))
							chunk_server.close()
							dead_chunks[loc] = 0
						except Exception as e:
							if "[Errno 111] Connection refused" in str(e):
								print('Connection to chunk server failed!')
								dead_chunks[loc] = 1
							else: print(e)

						if sum(dead_chunks) == config.NUM_CHUNKS:
							request = self._get_message_data('file_failed', dfs_dir, dfs_name)
							self.master.send(request)

				request = self._get_message_data("commit_delete", dfs_dir, dfs_name)
				self.master.send(request)
				self.master.recv(config.MESSAGE_SIZE)
				if response['status'] != 0:
					print(response['message'])
				else:
					print(f"Deleted {dfs_name}")
		except Exception as e:
			self.master.close()
			self.master_dead = True
			if "[Errno 32] Broken pipe" in str(e):
				print('Master not responding!')
			else: print(e)

		

	def close_connection(self):
			request = self._get_message_data('close', '')
			self.master.sendall(request)
			self.master.close()


	def _get_message_data(self, function, *args):
		function = function
		message = {
			'sender_type': 'client',
			'function': function,
			'args': args
		}

		encoded = json.dumps(message).encode('utf-8')
		encoded += b' ' * (config.MESSAGE_SIZE - len(encoded))
		return encoded


	def _get_status_data(self, status, message):
		message = {
			'status': status,
			'message': message
		}
		encoded = json.dumps(message).encode('utf-8')
		encoded += b' ' * (config.MESSAGE_SIZE - len(encoded))
		return encoded
		

if __name__ == '__main__':
	client = Client()
	while True:
		if client.master_dead:
			try:
				client.master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				client.master.connect((socket.gethostbyname('localhost'), config.MASTER_PORT))
				client.master_dead = False
				print('Connected to Master!')
			except:
				time.sleep(2)
				print('Master currently down. Reconnecting...')
		else:
			command = input('> ')
			words = command.split()
			command, args = words[0], words[1:]

			if command == 'ls':
				# usage ls <dfs_directory>
				if len(args) == 1:
					client.list_files(args[0])
				elif len(args) > 1:
					print('Many arguments found.')
				else:
					print('Too few arguments.')

			elif command == 'delete':
				# usage delete <dfs_directory> <dfs_name>
				if len(args) == 2:
					client.delete_file(args[0], args[1])
				elif len(args) > 2:
					print('Many arguments found.')
				else:
					print('Too few arguments.')

			elif command == 'read':
				# usage read <dfs_directory> <dfs_name>
				if len(args) == 2:
					client.read_file(args[0], args[1])
				elif len(args) > 2:
					print('Many arguments found.')
				else:
					print('Too few arguments.')

			elif command == 'create':
				# usage create <local_file> <dfs_directory> <dfs_name>
				if len(args) == 3:
					client.create_file(args[0], args[1], args[2])
				elif len(args) > 3:
					print('Many arguments found.')
				else:
					print('Too few arguments.')

			elif command == 'create_dir':
				# usage create_dir <dfs_directory> <directory_name>
				if len(args) == 2:
					client.create_dir(args[0], args[1])
				elif len(args) > 2:
					print('Many arguments found.')
				else:
					print('Too few arguments.')

			elif command == 'exit':
				client.close_connection()
				print("Exiting...")
				break
			else:
				print("Command not found!")