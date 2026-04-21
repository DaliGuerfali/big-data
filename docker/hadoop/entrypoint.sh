#!/bin/bash
set -e

NODE_TYPE=${HADOOP_NODE_TYPE:-namenode}

echo "Starting Hadoop node as: $NODE_TYPE"

if [ "$NODE_TYPE" = "namenode" ]; then
    # Format namenode only if not already formatted
    if [ ! -f /hadoop/dfs/name/current/VERSION ]; then
        echo "Formatting NameNode..."
        hdfs namenode -format -force -nonInteractive
    fi
    echo "Starting NameNode..."
    hdfs namenode
elif [ "$NODE_TYPE" = "datanode" ]; then
    # Wait for namenode to be ready
    echo "Waiting for NameNode..."
    until hdfs dfsadmin -safemode get 2>/dev/null; do
        echo "  NameNode not ready yet, retrying in 5s..."
        sleep 5
    done
    echo "Starting DataNode..."
    hdfs datanode
else
    echo "Unknown node type: $NODE_TYPE"
    exit 1
fi
