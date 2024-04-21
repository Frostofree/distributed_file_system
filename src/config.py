# General
HOST = "localhost"
CHUNK_SIZE = 64
MESSAGE_SIZE = 1024
PACKET_SIZE = 16384

# Master
MASTER_PORT = 8083
MASTER_SERVER = "localhost"
HEART_BEAT_INTERVAL = 2
PRUNING_INTERVAL = 5
MASTER_LOG = 'master.log'

# Chunk Server
NUM_CHUNKS = 3
CHUNK_PORTS = [7004, 7005, 7006]
# CHUNK_HEARBEAT_PORTS = [7004, 7005]
# CHUNK_HEALTH_PORTS = [8004, 8005]

REPLICATION_FACTOR = 2
ROOT_DIR = ['./chunkdata/chunk1', './chunkdata/chunk2', './chunkdata/chunk3', './chunkdata/chunk4', './chunkdata/chunk5']


