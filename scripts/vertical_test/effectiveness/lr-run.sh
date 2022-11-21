#!/bin/bash
ROOT="$(dirname $(dirname $(dirname $(pwd))))"
FLINK_DIR="$ROOT/Trisk-on-Flink/build-target/"
FLINK_APP_DIR="$ROOT/examples/"
JAR=${FLINK_APP_DIR}$"target/testbed-1.0-SNAPSHOT.jar"
### ### ###  		   ### ### ###

### ### ### INITIALIZATION ### ### ###

### ### ###  		   ### ### ###

init() {
  # app level
  DATA_ROOT="/home/drg/projects/work3/flink"
  LATENCY_DIR="${DATA_ROOT}/data/trisk/"
  ### paths configuration ###
  FLINK=$FLINK_DIR$"bin/flink"
  readonly SAVEPOINT_PATH="/home/drg/projects/work3/temp/"
  JOB="linearRoad.DailyExpenditure"
  EXP_NAME="DailyExpenditure"

  FILE_PATH="${DATA_ROOT}/histData/hist_lite.out"
  REQUEST_S=$3
  STATE_SIZE=100000
  SKEWNESS=$6

  PP=4
  STATE_P=${PP}
  FILTER_P=${PP}

  runtime=1200
  totalCachePerTM=500
  Controller=$1
  Group=$2
  Try=$4
  SUB_DIR=$Controller+$Group+$Try

  ROCKSDB_DIR="${DATA_ROOT}/rocksdb-storage"
  ROCKSDB_LOG_DIR=${ROCKSDB_DIR}"/logdir/"
  ROCKSDB_CHECKPOINT=${ROCKSDB_DIR}"/checkpoint/"
  ROCKSDB_DATA="${ROCKSDB_DIR}/localdir/"
  rm -rf ${ROCKSDB_DATA}*
  DATA_DIR="${DATA_ROOT}/data/perf/${EXP_NAME}"
#  sudo sh -c 'echo 1 > /proc/sys/vm/drop_caches'
#  sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
}

# config block cache size
function configApp() {
    echo "INFO: config app block cache size: ${totalCachePerTM}m"
#    sed -ri "s|(state.backend.rocksdb.block.cache-size: )[0-9]*|state.backend.rocksdb.block.cache-size: $totalCachePerTM|" ${FLINK_DIR}conf/flink-conf.yaml
#    sed -ri "s|(taskmanager.memory.managed.fraction: 0.)[0-9]*|taskmanager.memory.managed.fraction: 0.$totalCachePerTM|" ${FLINK_DIR}conf/flink-conf.yaml
    sed -ri "s|(trisk.taskmanager.managed_memory: )[0-9]*|trisk.taskmanager.managed_memory: $totalCachePerTM|" ${FLINK_DIR}conf/flink-conf.yaml
#    sed -i "s/^\(trisk.simple_test: \)\(true\|false\)/\1${simpleTest}/"  ${FLINK_DIR}conf/flink-conf.yaml
    sed -i "s/^\(trisk.controller: \)\(ElasticMemoryManager\|BlankController\)/\1${Controller}/"  ${FLINK_DIR}conf/flink-conf.yaml
}

function mvRocksdbLog() {
    if [[ ! -d ${DATA_DIR} ]]; then
            mkdir ${DATA_DIR}
    fi
    mkdir ${DATA_DIR}/${SKEWNESS}
    if [[ -d ${DATA_DIR}/${SKEWNESS}/${SUB_DIR} ]]; then
            # shellcheck disable=SC2115
            rm -rf ${DATA_DIR}/${SKEWNESS}/${SUB_DIR}
    fi
    mkdir ${DATA_DIR}/${SKEWNESS}/${SUB_DIR}
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
  mv ${FLINK_DIR}log/* ${DATA_DIR}/${SKEWNESS}/${SUB_DIR}
  mv ${LATENCY_DIR}* ${DATA_DIR}/${SKEWNESS}/${SUB_DIR}
  rm -rf /tmp/flink*
  rm ${FLINK_DIR}log/*
}

function cleanRocksdbLog() {
    rm -rf ${ROCKSDB_LOG_DIR}*
    rm -rf ${ROCKSDB_CHECKPOINT}*
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
  echo "INFO: $FLINK run -c ${JOB} ${JAR} -request-rate ${REQUEST_S} -p-state ${STATE_P} -p-filter ${FILTER_P} -state-size ${STATE_SIZE} -keys ${KEY_SIZE} -group-all ${Group} -skewness ${SKEWNESS} -hist-file ${FILE_PATH} &"
  rm nohup.out
  nohup $FLINK run -c ${JOB} ${JAR} -request-rate ${REQUEST_S} -p-state ${STATE_P} -p-filter ${FILTER_P} -state-size ${STATE_SIZE} -keys ${KEY_SIZE} -group-all ${Group} -skewness ${SKEWNESS} -hist-file ${FILE_PATH} &
}

# run one flink demo exp, which is a word count job
run_one_exp() {
  configApp

  # compute n_tuples from per task rates and parallelism
  echo "INFO: run exp Nexmark exchange"
#  configFlink
  cleanRocksdbLog
  runFlink
  python3 -c 'import time; time.sleep(5)'

  runApp

  SCRIPTS_RUNTIME=`expr ${runtime} - 50 + 10`
  python3 -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'
  stopFlink
}

test() {
#  configApp
  mvRocksdbLog
}

init $1 $2 $3 $4 $5 $6
run_one_exp
#test
