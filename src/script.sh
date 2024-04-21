#!/bin/bash

rm -r ./chunkdata/
# Define the port number
PORT_NUMBER=8083

# Step 1: Identify the process using the port
PROCESS_ID=$(lsof -ti:$PORT_NUMBER)

if [ -z "$PROCESS_ID" ]; then
    echo "No process found using port $PORT_NUMBER"
fi

# Step 2: Stop the process
echo "Stopping process with ID: $PROCESS_ID"
if kill -9 $PROCESS_ID; then
    echo "Process stopped successfully"
else
    echo "Failed to stop process"
fi

PORT_NUMBER=8004

PROCESS_ID=$(lsof -ti:$PORT_NUMBER)

if [ -z "$PROCESS_ID" ]; then
    echo "No process found using port $PORT_NUMBER"
fi

echo "Found process with ID: $PROCESS_ID"

# Try to stop the process
if kill -9 $PROCESS_ID; then
    echo "Process stopped successfully"
else
    echo "Failed to stop process"
fi

PORT_NUMBER=8005

PROCESS_ID=$(lsof -ti:$PORT_NUMBER)

if [ -z "$PROCESS_ID" ]; then
    echo "No process found using port $PORT_NUMBER"
fi

echo "Found process with ID: $PROCESS_ID"

# Try to stop the process
if kill -9 $PROCESS_ID; then
    echo "Process stopped successfully"
else
    echo "Failed to stop process"
fi

PORT_NUMBER=8006
PROCESS_ID=$(lsof -ti:$PORT_NUMBER)
kill -9  $PROCESS_ID
