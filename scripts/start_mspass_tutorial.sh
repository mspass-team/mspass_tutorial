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
$SPARK_HOME/sbin/start-slave.sh spark://0.0.0.0:$SPARK_MASTER_PORT
[[ -d $MONGO_DATA ]] || mkdir $MONGO_DATA
mongod --dbpath $MONGO_DATA --logpath $MONGO_LOG --bind_ip_all &
if [ $# -eq 0 ]; then
  pyspark \
    --conf "spark.mongodb.input.uri=mongodb://0.0.0.0/test.myCollection?readPreference=primaryPreferred" \
    --conf "spark.mongodb.output.uri=mongodb://0.0.0.0/test.myCollection" \
    --conf "spark.master=spark://0.0.0.0:7077" \
    --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1
else
  pyspark $@
fi