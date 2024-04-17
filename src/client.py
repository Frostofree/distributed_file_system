import socket
import pickle
import os.path

import config

MAX = 10000

def stream_chunk(local_path, curr, handle, locs):
	yield handle
	yield locs
	with open(local_path, 'r', buffering=config.PACKET_SIZE) as f:
		f.seek(curr*config.CHUNK_SIZE)
		for _ in range(config.CHUNK_SIZE // config.PACKET_SIZE):
			data = f.read(config.PACKET_SIZE)
			if len(data) == 0:
				break
			yield data

class Client():
	def __init__(self):
		self.master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.master.connect((socket.gethostbyname('localhost'), config.MASTER_PORT))
		self.chunks = []
		# for i in range(config.NUM_CHUNKS):
		# 	chunk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		# 	chunk.connect((socket.gethostbyname('localhost'), config.CHUNK_PORTS[i]))
		# 	self.chunks.append(chunk)

	def create_file(self, local_path, dfs_path):
		bytes = str(os.path.getsize(local_path))
		chunks = bytes // config.CHUNK_SIZE + int(bytes%config.CHUNK_SIZE != 0)
		request = 'client:' + 'create_file:' + dfs_path + ':lol2'
		self.master.send(bytes(request, 'utf-8'))
		status = self.master.recv(1024)
		message = pickle.loads(status)
		# check for errors
		curr = 0
		flag = True
		while curr < chunks:
			request = 'client:' + 'get_chunk_locs:' + dfs_path + ':' + curr
			self.master.send(bytes(request, 'utf-8'))
			status = self.master.recv(1024)
			message = pickle.loads(status)
			# check for errors
			chunk = message
			iterator = stream_chunk(local_path, curr, chunk.handle, chunk.locs)
			request = 'client:' + 'create_chunk:' + iterator + ':lol2'
			self.chunks[chunk.locs[0]].send(bytes(request, 'utf-8'))
			status = self.chunks[chunk.locs[0]].recv(1024)
			message = pickle.loads(status)
			# handle for errors and retrying here
			request = 'client:' + 'commit_chunk:' + dfs_path + ':' + chunk.handle
			self.master.send(bytes(request, 'utf-8'))
			status = self.master.recv(1024)
			message = pickle.loads(status)
			curr += 1
		request = 'client:' + 'file_create_status:' + flag + ':lol2'
		self.master.send(bytes(request, 'utf-8'))
		status = self.master.recv(1024)
		message = pickle.loads(status)
		print(message)


	def read_file(self, path, offset, bytes):
		if offset < 0 or bytes < -1:
			print('Invalid arguments')
			return
		start = offset // config.CHUNK_SIZE
		end = -1 if bytes == -1 else ((offset+start-1) // config.CHUNK_SIZE)
		curr = start
		while end == -1 or curr <= end:
			request = 'client:' + 'get_chunk_details:' + path + ':' + curr
			self.master.send(bytes(request, 'utf-8'))
			status = self.master.recv(1024)
			message = pickle.loads(status)
			if 'failure' in message:
				if end != -1 or curr == start:
					print('Failed to fetch file')
				print('')
				return
			begin = 0 if curr != start else (offset % config.CHUNK_SIZE)
			end = config.CHUNK_SIZE
			for i in message.locs:
				data = ''
				request = 'client:' + 'read_chunk:' + str(begin) + ':' + str(end-begin+1)
				self.chunks[i].send(bytes(request, 'utf-8'))
				status = self.chunks[i].recv(1024)
				iterator = pickle.loads(status)
				for it in iterator:
					data += it
				print(data, end='')
			curr += 1
		print('')
		

	def list_files(self): 
		request = 'client:' + 'list_files:' + 'lol1:' + 'lol2'
		self.master.send(bytes(request, 'utf-8'))
		status = self.master.recv(1024)
		iterator = pickle.loads(status)
		for it in iterator:
			print(it, end=' ')
		print()


	def delete_file(self, path):
		request = 'client:' + 'delete_file:' + path + ':lol2'
		self.master.send(bytes(request, 'utf-8'))
		status = self.master.recv(1024)
		message = pickle.loads(status)
		if 'failure' in message:
			print('Could not delete file!')
		else: print('File deleted successfully.')


if __name__ == '__main__':
	client = Client()
	while True:
		command = input('> ')
		words = command.split()
		command, args = words[0], words[1:]

		if command == 'ls':
			client.list_files()
		elif command == 'delete':
			client.delete_file(args[0])
		elif command == 'read':
			client.read_file(args[0], int(args[1]), int(args[2]))
		elif command == 'create':
			client.create_file(args[0], args[1])
		elif command == 'exit':
			break
		else:
			print('Please enter a valid command')


# create local_file_path dfs_file_path
# 	get the size of files
# 	send message to master to create file (create_file)
# 	for each byte
# 		send message to master to get chunk location (get_chunk_locs)
# 		preprocess the data and create the request iterator
# 		send message to chunk_server to store the new chunk (create_chunk)
# 		if successful send message to master to commit chunk (commit_chunk)
# 		else retry it for a fixed number of times
# 	if all bytes done then send file create status msg to master (file_create_status)
# 	else send file create status msg to master (file_create_status)

# ls int
# 	send message to master (list_files)
# 	get the iterator
# 	print the files using iterator

# read file_path offset num_bytes
# 	check for edge cases
# 	for each byte
# 		get metadata from the master (get_chunk_details)
# 		get the file contents from the respective chunk_servers (read_chunk)
# 		print the data

# delete file_path
# 	send delete msg to master (delete_file)



# import socket
# import pickle

# import config

# class Client():
# 	def __init__(self):
# 		self.master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# 		self.master.connect((socket.gethostbyname('localhost'), config.MASTER_PORT))
# 		self.chunks = []
# 		for i in range(config.NUM_CHUNKS):
# 			chunk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# 			chunk.connect((socket.gethostbyname('localhost'), config.CHUNK_PORTS[i]))
# 			self.chunks.append(chunk)

# 	def create_file(self, local_path, dfs_path):
# 		pass

# 	def read_file(self, path, offset, bytes):
# 		if offset < 0 or bytes < -1:
# 			print('Invalid arguments')
# 			return
# 		start = offset // config.CHUNK_SIZE
# 		end = -1 if bytes == -1 else ((offset+start-1) // config.CHUNK_SIZE)
# 		curr = start
# 		while end == -1 or curr <= end:
# 			request = 'client:' + 'readfile:' + curr + ':lol2'
# 			self.master.send(bytes(request, 'utf-8'))
# 			status = self.master.recv(1024)
# 			message = pickle.loads(status)
# 			if 'failure' in message:
# 				if end != -1 or curr == start:
# 					print('Failed to fetch file')
# 				print('')
# 				return
# 			begin = 0 if curr != start else (offset % config.CHUNK_SIZE)
# 			end = config.CHUNK_SIZE
# 			# request = 
# 			curr += 1
# 		pass

# 	def list_files(self):
# 		request = 'client:' + 'listfiles:' + 'lol1:' + 'lol2'
# 		self.master.send(bytes(request, 'utf-8'))
# 		status = self.master.recv(1024)
# 		iterator = pickle.loads(status)
# 		for it in iterator:
# 			print(it, end=' ')
# 		print()


# 	def delete_file(self, path):
# 		request = 'client:' + 'deletefile:' + path + ':lol2'
# 		self.master.send(bytes(request, 'utf-8'))
# 		status = self.master.recv(1024)
# 		message = pickle.loads(status)
# 		if 'failure' in message:
# 			print('Could not delete file!')
# 		else: print('File deleted successfully.')


# if __name__ == '__main__':
# 	client = Client()
# 	while True:
# 		command = input('> ')
# 		words = command.split()
# 		command, args = words[0], words[1:]

# 		if command == 'ls':
# 			client.list_files()
# 		elif command == 'delete':
# 			client.delete_file(args[0])
# 		elif command == 'read':
# 			client.read_file(args[0], int(args[1]), int(args[2]))
# 		elif command == 'create':
# 			client.create_file(args[0], args[1])
# 		elif command == 'exit':
# 			break
# 		else:
# 			print('Please enter a valid command')


# # create local_file_path dfs_file_path
# # 	get the number of files
# # 	send message to master to create file (create_file)
# # 	for each byte
# # 		send message to master to get chunk location (get_chunk_locs)
# # 		preprocess the data and create the request iterator
# # 		send message to chunk_server to store the new chunk (create_chunk)
# # 		if successful send message to master to commit chunk (commit_chunk)
# # 		else retry it for a fixed number of times
# # 	if all bytes done then send file create status msg to master (file_create_status)
# # 	else send file create status msg to master (file_create_status)

# # ls int
# # 	send message to master (list_files)
# # 	get the iterator
# # 	print the files using iterator

# # read file_path offset num_bytes
# # 	check for edge cases
# # 	for each byte
# # 		get metadata from the master (get_chunk_details)
# # 		get the file contents from the respective chunk_servers (read_chunk)
# # 		print the data

# # delete file_path
# # 	send delete msg to master (delete_file)
