#!/bin/bash
set -e
cd /opt/hive/data
mkdir -p /opt/hive/data/scratch_dir /opt/hive/data/install_dir
chmod 777 /opt/hive/data/scratch_dir /opt/hive/data/install_dir
if [ ! -f metastore_db/service.properties ]; then
  rm -rf metastore_db
  /opt/hive/bin/schematool -dbType derby -initSchema
fi
export HADOOP_CLASSPATH="${TEZ_HOME}/*:${TEZ_HOME}/lib/*:${HADOOP_CLASSPATH:-}"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Xmx1G"
exec /opt/hive/bin/hive --skiphadoopversion --skiphbasecp --service hiveserver2
