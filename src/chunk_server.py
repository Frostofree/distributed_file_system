import sys
import socket
import pickle
import os.path
import threading

import config

def preprocess(args):
	chunk_handle, loc_list = None, None
	for it in args:
		chunk_handle = it
		break
	for it in args:
		loc_list = it
		break
	return chunk_handle, loc_list, args

def parse_message(message):
		# sample message: client:' + 'createfile:' + dfs_path + ':' + size
		parsed = message.split(':')
		sender_type = parsed[0]
		command = parsed[1]
		args = parsed[2:]
		return sender_type, command, args

def stream_chunk(file_path, offset, num_bytes):
	curr = 0
	with open(file_path, 'r', buffering=config.PACKET_SIZE) as f:
		f.seek(offset)
		while curr < num_bytes:
			data = f.read(min(config.PACKET_SIZE, num_bytes-curr))
			curr += config.PACKET_SIZE
			if len(data) == 0:
				break
			yield data


class ChunkServer():
	def __init__(self, host, port, rootdir):
		self.host = host
		self.port = port
		self.rootdir = rootdir
		self.present = {}
		self.lock = threading.Lock()
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.sock.bind((self.host, self.port))
		self.wake_up(1)

	def wake_up(self, init):
		pass

	def read_chunk(self, client, chunk_handle, offset, num_bytes):
		with self.lock:
			if chunk_handle not in self.present.keys():
				raise OSError('Chunk Not Found!')
		if not self.present[chunk_handle]:
			raise OSError('Chunk is busy...')
		file_path = os.path.join(self.rootdir, chunk_handle)
		message = stream_chunk(file_path, offset, num_bytes)
		client.send(pickle.dumps(message))
		

	def write_chunk(self, client, chunk_handle, iterator):
		file_path = os.path.join(self.rootdir, chunk_handle)
		with open(file_path, 'w') as f:
			with self.lock:
				self.present[chunk_handle] = False
			for it in iterator:
				f.write(it.str)
		message = 'Chunk created'
		client.send(pickle.dumps(message))
		

	def write_and_yield_chunk(self, chunk_handle, loc_list, iterator):
		yield chunk_handle
		yield loc_list
		file_path = os.path.join(self.rootdir, chunk_handle)
		with open(file_path, 'w') as f:
			with self.lock:
				self.present[chunk_handle] = False
			for it in iterator:
				f.write(it.str)
				yield it
		

	def create_chunk(self, client, chunk_handle, loc_list, iterator):
		loc_list.pop(0)
		if not loc_list:
			self.write_chunk(client, chunk_handle, iterator)
		else:
			iterator = self.write_and_yield_chunk(chunk_handle, loc_list, iterator)
			nextChunk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			nextChunk.connect((socket.gethostbyname('localhost'), config.CHUNK_PORTS[loc_list[0]]))
			request = 'chunkserver:' + 'create_chunk:' + iterator + ':lol2'
			nextChunk.send(bytes(request, 'utf-8'))
			status = nextChunk.recv(1024)
			client.send(status)
			message = pickle.loads(status)
			print(message)
            

	def replicate_chunk(self, client, chunk_loc, chunk_handle):
		nextChunk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		nextChunk.connect((socket.gethostbyname('localhost'), config.CHUNK_PORTS[chunk_loc]))
		request = 'chunkserver:' + 'read_entire_chunk:' + chunk_handle + ':lol2'
		nextChunk.send(bytes(request, 'utf-8'))
		status = nextChunk.recv(1024)
		iterator = pickle.loads(status)
		self.read_chunk(client, chunk_handle, iterator)
		

	def commit_chunk(self, client, chunk_handle):
		with self.lock:
			self.present[chunk_handle] = True
		client.send(pickle.dumps('Commmitted'))
		

	def delete_chunk(self, client, iterator):
		for it in iterator:
			chunk_handle = it.str
			file_path = os.path.join(self.rootdir, chunk_handle)
			os.remove(file_path)
			with self.lock:
				self.present.pop(chunk_handle, None)
		client.send(pickle.dumps('Deleted'))
		

	def chunk_cleanup(self):
		pass
    

	def listen(self):
		self.sock.listen(5)
		while True:
			client, address = self.sock.accept()
			client.timeout(60)
			handler = threading.Thread(target=self.service, args=(client, address))
			handler.start()

	def service(self, client, address):
		# listen here fron client and master
		sender_type, command, args = parse_message(client.recv(1024).decode('utf-8'))
		if sender_type == 'master':
			if command == 'commit_chunk':
				self.commit_chunk(client, args[0].str)
			elif command == 'replicate_chunk':
				chunk_loc, chunk_handle = args[0].str.split('$')
				self.replicate_chunk(client, chunk_loc, chunk_handle)
			elif command == 'delete_chunk':
				self.delete_chunk(client, args[0])
			elif command == 'heartbeat':
				client.send(pickle.dumps('Present'))
		elif sender_type == 'client':
			if command == 'create_chunk':
				chunk_handle, loc_list, iterator = preprocess(args[0])
				self.create_chunk(client, chunk_handle, loc_list, iterator)
			elif command == 'read_chunk':
				self.read_chunk(client, args[0], int(args[1]), int(args[2]))
		elif sender_type == 'chunkserver':
			if command == 'read_entire_chunk':
				self.read_chunk(client, args[0].str, 0, config.CHUNK_SIZE)
			elif command == 'create_chunk':
				chunk_handle, loc_list, iterator = preprocess(args[0])
				self.create_chunk(client, chunk_handle, loc_list, iterator)
		else:
			print('Invalid Sender Type!')


if __name__ == '__main__':
	num = int(sys.argv[1])
	if num < 0 and num > 3:
		print(f'Enter a valid Port number in [0, {config.NUM_CHUNKS-1}]')
		exit()
		
	port = config.CHUNK_PORTS[num]
	rootdir = config.ROOT_DIR[num]

	print('Chunk Server Running')
	cs = ChunkServer(config.HOST, port, rootdir)
	cs.listen()