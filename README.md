# DFS - Distributed File System

# Running the distributed file system

## Master

To run the master server, run the command:

```bash
python3 master.py
```

## Chunk server

To run the chunk servers, run the command 

```bash
python3 chunk_server.py [0-4]
```

The [0-4] number indicates the id of the chunk server. Currently, there can be 5 chunk servers run.

This can be modified in the [config.py](http://config.py) file.

## Client

To run the client server, run the command:

```bash
python3 client.py
```

To perform an operation, run the following commands:

- Create a new file in the distributed file system.

```bash
create <local-file> <dfs-dir> <dfs-file>
```

- Create a new directory in the distributed file system.

```bash
mkdir <dfs-dir> <dir-name>
```

- Read a file from the distributed file system.

```bash
read <dfs-dir> <dfs-name>
```

- Delete a file from the distributed file system.

```bash
delete <dfs-dir> <dfs-file>
```

- List all the files and directories of a directory in distributed file system.

```bash
ls <dfs-dir>
```

## Functionalities:

- Consistency Model:
The distributed file system implemented follows a strict consistency model, ensuring that all clients see the same view of the file system at any given time. This is achieved through the use of a centralized master server, which acts as the single point of control and coordination for all file system operations.

*File Creation*:

1. When a client wants to create, it first obtains an exclusive lock on the file from the master server (create_file command).
2. If the file already exists and is committed, the master server denies the request to prevent concurrent modifications.
3. If the file is being created or modified by another client, the master server denies the request, ensuring that only one client can create or write to a file at a time.
4. Once the client has obtained the lock, it can proceed with writing the file data to the chunk servers.
5. After writing all the chunks, the client commits the file to the master server (commit_file command), which then releases the lock.

*Read*:

1. When a client wants to read a file, it obtains a shared lock on the file from the master server (read_file command).
2. The master server ensures that the file is in a committed state before granting the lock.
3. If the file is being deleted or modified by another client, the master server denies the read request to maintain consistency.
4. Once the client has obtained the lock, it can read the file data from the chunk servers.
5. After reading the file, the client releases the lock by sending a message to the master server.
6. At a given point of time, any number of clients can read the same file.

*Delete*:

1. When a client wants to delete a file, it obtains an exclusive lock on the file from the master server (delete_file command).
2. The master server ensures that the file is in a committed state before granting the lock.
3. If the file is being read or modified by another client, the master server denies the delete request to maintain consistency.
4. Once the client has obtained the lock, it can delete the chunks from the chunk servers.
5. After deleting all the chunks, the client commits the deletion to the master server (commit_delete command), which then releases the lock and removes the file metadata.

- Directory Consistency:
The code ensures directory consistency by checking the existence of directories and sub-directories before performing any file operations. If a directory or subdirectory does not exist, the master server denies the requested operation and returns an appropriate error message to the client.

- Logging and Recovery:
The master server maintains a log file (master.log) to persist the state of the file system. This log file records all file system operations, such as file creation, directory creation, chunk locations, and deletions. Upon startup, the master server restores the file system state from the log file, ensuring that the system remains consistent even after failures or restarts.

- Fault Tolerance and Replication:
The code implements fault tolerance through chunk replication and heartbeat mechanisms:
    1. Each file chunk is replicated across multiple chunk servers (configurable replication factor) to provide redundancy.
    2. The master server periodically performs heartbeat checks on the chunk servers to detect failures.
    3. If a chunk server fails, the master server can initiate the replication of the affected chunks on a new chunk server to maintain the desired replication factor and data availability.

- Concurrency Control:
The code uses a synchronized dictionary (SynchronizedDict) to manage file locks across multiple clients. This ensures that only one client can hold an exclusive lock on a file at a time, preventing concurrent modifications and maintaining data consistency.
