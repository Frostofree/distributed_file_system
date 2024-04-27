HOST = "localhost"
CHUNK_SIZE = 4096
MESSAGE_SIZE = 8192
PACKET_SIZE = 16384


MASTER_PORT = 8090
MASTER_SERVER = "localhost"
HEART_BEAT_INTERVAL = 5
HEART_BEAT_TIMEOUT = 10
PRUNING_INTERVAL = 5
MASTER_LOG = 'master.log'


NUM_CHUNKS = 6
CHUNK_PORTS = [8000, 8001, 8002, 8003, 8004, 8005]

REPLICATION_FACTOR = 3
ROOT_DIR = ['./chunkdata/chunk0', './chunkdata/chunk1', './chunkdata/chunk2', './chunkdata/chunk3', './chunkdata/chunk4', './chunkdata/chunk5']
