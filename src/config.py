# General
HOST = "localhost"
CHUNK_SIZE = 64
MESSAGE_SIZE = 1024
PACKET_SIZE = 16384

# Master
MASTER_PORT = 8083
MASTER_SERVER = "10.2.129.199"
HEART_BEAT_INTERVAL = 2
MASTER_LOG = 'master.log'

# Chunk Server
NUM_CHUNKS = 5
CHUNK_PORTS = [7004, 7005, 7006, 7007, 7008]
# CHUNK_HEARBEAT_PORTS = [7004, 7005]
# CHUNK_HEALTH_PORTS = [8004, 8005]
REPLICATION_FACTOR = 3
ROOT_DIR = ['./chunkdata/chunk1', './chunkdata/chunk2', './chunkdata/chunk3', './chunkdata/chunk4', './chunkdata/chunk5']


