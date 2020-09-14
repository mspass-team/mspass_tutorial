#!/bin/bash

if grep docker /proc/1/cgroup -qa; then
  export SPARK_LOG_DIR=/home
  MONGO_DATA=/home/data
  MONGO_LOG=/home/mongo_log
else
  export SPARK_LOG_DIR=$PWD
  MONGO_DATA=${PWD%/}/data
  MONGO_LOG=${PWD%/}/mongo_log
fi

$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-slave.sh spark://127.0.0.1:$SPARK_MASTER_PORT
[[ -d $MONGO_DATA ]] || mkdir $MONGO_DATA
mongod --dbpath $MONGO_DATA --logpath $MONGO_LOG --bind_ip_all &
if [ $# -eq 0 ]; then
  jupyter notebook --port=8888 --no-browser --ip=0.0.0.0 --allow-root
else
  jupyter $@
fi