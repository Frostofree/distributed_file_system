# General
HOST = 'localhost'
CHUNK_SIZE = 16
PACKET_SIZE = 4

# Master
MASTER_PORT = 8000

# Chunk Server
NUM_CHUNKS = 4
CHUNK_PORTS = [8001, 8002, 8003, 8004]
REPLICATION_FACTOR = 2
ROOT_DIR = ['/chunk1', '/chunk2', '/chunk3', '/chunk4']