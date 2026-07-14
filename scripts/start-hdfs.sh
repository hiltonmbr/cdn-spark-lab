#!/bin/bash
if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
    echo "Formatting NameNode..."
    echo "Y" | hdfs namenode -format -force
fi
hdfs namenode
