#!/bin/bash

### ### ###  		   ### ### ###

### ### ### INITIALIZATION ### ### ###

### ### ###  		   ### ### ###


init() {
  # app level
  FLINK_DIR="/home/drg/projects/work3/flink/Trisk/Trisk-on-Flink/build-target/"
  FLINK_APP_DIR="/home/drg/projects/work3/flink/Trisk/examples/"
  JAR=${FLINK_APP_DIR}$"target/testbed-1.0-SNAPSHOT.jar"
  ### paths configuration ###
  FLINK=$FLINK_DIR$"bin/flink"
  readonly SAVEPOINT_PATH="/home/drg/projects/work3/temp/"
  JOB="stock.InAppStatefulStockExchange"
  EXP_NAME="stock"

  partitions=128
  parallelism=1
  runtime=300
  blockCacheSize=$1
  ROCKSDB_LOG_DIR="/home/drg/projects/work3/flink/rocksdb-storage/logdir/"
  DATA_DIR="/home/drg/projects/work3/flink/data/${EXP_NAME}"
}

# config block cache size
function configApp() {
    echo "INFO: config app block cache size: ${blockCacheSize}m"
#    sed -ri "s|(state.backend.rocksdb.block.cache-size: )[0-9]*|state.backend.rocksdb.block.cache-size: $blockCacheSize|" ${FLINK_DIR}conf/flink-conf.yaml
    sed -ri "s|(taskmanager.memory.managed.fraction: 0.)[0-9]*|taskmanager.memory.managed.fraction: 0.$blockCacheSize|" ${FLINK_DIR}conf/flink-conf.yaml
}

function mvRocksdbLog() {
    if [[ ! -d ${DATA_DIR} ]]; then
            mkdir ${DATA_DIR}
    fi
    if [[ -d ${DATA_DIR}/${blockCacheSize} ]]; then
            # shellcheck disable=SC2115
            rm -rf ${DATA_DIR}/${blockCacheSize}
    fi
    mkdir ${DATA_DIR}/${blockCacheSize}
    mv ${ROCKSDB_LOG_DIR}* ${DATA_DIR}/${blockCacheSize}
#    for entry in ${ROCKSDB_LOG_DIR}*
#    do
#      echo "$entry"
#    done
    echo "INFO: move rocksdb Log"

}

# run flink clsuter
function runFlink() {
    echo "INFO: starting the cluster"
    if [[ -d ${FLINK_DIR}log ]]; then
        rm -rf ${FLINK_DIR}log
    fi
    mkdir ${FLINK_DIR}log
    ${FLINK_DIR}/bin/start-cluster.sh
}

# clean app specific related data
function cleanEnv() {
  mvRocksdbLog
#  if [[ -d ${FLINK_DIR}${EXP_NAME} ]]; then
#      rm -rf ${FLINK_DIR}${EXP_NAME}
#  fi
#  mv ${FLINK_DIR}log ${FLINK_DIR}${EXP_NAME}
  mv ${FLINK_DIR}log/* ${DATA_DIR}/${blockCacheSize}
  rm -rf /tmp/flink*
  rm ${FLINK_DIR}log/*
}

# clsoe flink clsuter
function stopFlink() {
    echo "INFO: experiment finished, stopping the cluster"
    PID=`jps | grep CliFrontend | awk '{print $1}'`
    if [[ ! -z $PID ]]; then
      kill -9 ${PID}
    fi
    PID=`jps | grep StockGenerator | awk '{print $1}'`
    if [[ ! -z $PID ]]; then
      kill -9 ${PID}
    fi
    ${FLINK_DIR}bin/stop-cluster.sh
    echo "close finished"
    cleanEnv
}


# run applications
function runApp() {
  echo "INFO: $FLINK run -c ${JOB} ${JAR} -p2 ${parallelism} -mp2 ${partitions} &"
  rm nohup.out
  nohup $FLINK run -c ${JOB} ${JAR} -p2 ${parallelism} -mp2 ${partitions} &
}

function runGenerator() {
  echo "INFO: java -cp ${FLINK_APP_DIR}target/testbed-1.0-SNAPSHOT.jar kafkagenerator.StockGenerator > /dev/null 2>&1 &"
  java -cp ${FLINK_APP_DIR}target/testbed-1.0-SNAPSHOT.jar kafkagenerator.StockGenerator > /dev/null 2>&1 &
}

# run applications
function reconfigApp() {
  JOB_ID=$(cat nohup.out | sed -n '1p' | rev | cut -d' ' -f 1 | rev)
  JOB_ID=$(echo $JOB_ID |tr -d '\n')
  echo "INFO: running job: $JOB_ID"

  savepointPathStr=$($FLINK cancel -s $SAVEPOINT_PATH $JOB_ID)
  savepointFile=$(echo $savepointPathStr| rev | cut -d'/' -f 1 | rev)
  x=$(echo $savepointFile |tr -d '.')
  x=$(echo $x |tr -d '\n')

  rm nohup.out
  echo "INFO: RECOVER $FLINK run -d -s $SAVEPOINT_PATH$x -c ${JOB} ${JAR} -p2 ${parallelism} -mp2 ${partitions} &"
  nohup $FLINK run -d -s $SAVEPOINT_PATH$x --class $JOB $JAR  -p2 ${parallelism} -mp2 ${partitions} &
}

# run one flink demo exp, which is a word count job
run_one_exp() {
  configApp

  # compute n_tuples from per task rates and parallelism
  echo "INFO: run exp stock exchange"
#  configFlink
  runFlink
  python3 -c 'import time; time.sleep(5)'

  runApp

#  SCRIPTS_RUNTIME=`expr ${runtime} - 50 + 10`
  SCRIPTS_RUNTIME=${runtime}
  python3 -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'
  stopFlink
}

test() {
#  configApp
  mvRocksdbLog
}

init $1
run_one_exp
#test
