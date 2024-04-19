# General
HOST = 'localhost'
CHUNK_SIZE = 32
MESSAGE_SIZE = 1024
PACKET_SIZE = 2048

# Master
MASTER_PORT = 8005

# Chunk Server
NUM_CHUNKS = 1
CHUNK_PORTS = [8001]
REPLICATION_FACTOR = 1
ROOT_DIR = ['./chunkdata/chunk1', './chunkdata/chunk2', './chunkdata/chunk3', './chunkdata/chunk4']