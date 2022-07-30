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
  DATA_ROOT="/data/EMM_data"
  LATENCY_DIR="${DATA_ROOT}/data/trisk/"
  if  [[ "$2" = "blank" ]]; then
    FLINK_DIR="$ROOT/../../flink/build-target/"
  fi
  ### paths configuration ###
  FLINK=$FLINK_DIR$"bin/flink"
  readonly SAVEPOINT_PATH="/home/drg/projects/work3/temp/"
  if  [[ "$1" = "q20" ]]; then
    JOB="Nexmark.queries.Query20"
  elif [[ "$1" = "q4" ]]; then
    JOB="Nexmark.queries.Query4"
  fi
  EXP_NAME="nexmark-$1"

  AUCTION_S=0
  PERSON_S=3000
  BID_S=500
  STATE_SIZE=100000
  KEY_SIZE=50000
  SKEWNESS=1

  PP=4
  AUCTION_P=${PP}
  PERSON_P=${PP}
  BID_P=${PP}
  JOIN_P=${PP}
  WIN_P=${PP}
  FILTER_P=${PP}

  runtime=1200
  totalCachePerTM=500
  Controller="ElasticMemoryManager"
  Group="false"
  Try=$1
  SUB_DIR1=${BID_S}
  SUB_DIR2=$Controller+$Try

  ROCKSDB_DIR="${DATA_ROOT}/rocksdb-storage"
  ROCKSDB_LOG_DIR=${ROCKSDB_DIR}"/logdir/"
  ROCKSDB_CHECKPOINT=${ROCKSDB_DIR}"/checkpoint/"
  ROCKSDB_DATA="${ROCKSDB_DIR}/localdir/"
  rm -rf ${ROCKSDB_DATA}*
  DATA_DIR="${DATA_ROOT}/data/${EXP_NAME}"
#  DATA_DIR="/home/drg/projects/work3/flink/data/${EXP_NAME}/queue_delay"
#  DATA_DIR="/home/drg/projects/work3/flink/data/${EXP_NAME}/state_size/${readCount}"
#  sudo sh -c 'echo 1 > /proc/sys/vm/drop_caches'
#  sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
}

# config block cache size
function configApp() {
    echo "INFO: config app block cache size: ${totalCachePerTM}m"
    sed -ri "s|(trisk.taskmanager.managed_memory: )[0-9]*|trisk.taskmanager.managed_memory: $totalCachePerTM|" ${FLINK_DIR}conf/flink-conf.yaml
    sed -i "s/^\(trisk.controller: \)\(ElasticMemoryManager\|BlankController\|TestInitMemoryManager\)/\1${Controller}/"  ${FLINK_DIR}conf/flink-conf.yaml
}

function mvRocksdbLog() {
    if [[ ! -d ${DATA_DIR} ]]; then
            mkdir ${DATA_DIR}
    fi
    mkdir ${DATA_DIR}/${SUB_DIR1}
    if [[ -d ${DATA_DIR}/${SUB_DIR1}/${SUB_DIR2} ]]; then
            # shellcheck disable=SC2115
            rm -rf ${DATA_DIR}/${SUB_DIR1}/${SUB_DIR2}
    fi
    mkdir ${DATA_DIR}/${SUB_DIR1}/${SUB_DIR2}
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
  mv ${FLINK_DIR}log/* ${DATA_DIR}/${SUB_DIR1}/${SUB_DIR2}
  mv ${LATENCY_DIR}* ${DATA_DIR}/${SUB_DIR1}/${SUB_DIR2}
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
  echo "INFO: $FLINK run -c ${JOB} ${JAR} -auction-srcRate ${AUCTION_S} -person-srcRate ${PERSON_S} -bid-srcRate ${BID_S} -p-auction-source ${AUCTION_P} -p-person-source ${PERSON_P} -p-bid-source ${BID_P} -p-join ${JOIN_P} -p-window ${WIN_P} -p-filter ${FILTER_P} -state-size ${STATE_SIZE} -keys ${KEY_SIZE} -group-all ${Group} -skewness ${SKEWNESS} &"
  rm nohup.out
  nohup $FLINK run -c ${JOB} ${JAR} -auction-srcRate ${AUCTION_S} -person-srcRate ${PERSON_S} -bid-srcRate ${BID_S} -p-auction-source ${AUCTION_P} -p-person-source ${PERSON_P} -p-bid-source ${BID_P} -p-join ${JOIN_P} -p-window ${WIN_P} -p-filter ${FILTER_P} -state-size ${STATE_SIZE} -keys ${KEY_SIZE} -group-all ${Group} -skewness ${SKEWNESS} &
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
  echo "INFO: RECOVER $FLINK run -d -s $SAVEPOINT_PATH$x -c ${JOB} ${JAR} -auction-srcRate ${AUCTION_S} -person-srcRate ${PERSON_S} -p-auction-source ${AUCTION_P} -p-person-source ${PERSON_P} -p-join ${JOIN_P} &"
  nohup $FLINK run -d -s $SAVEPOINT_PATH$x --class $JOB $JAR  -auction-srcRate ${AUCTION_S} -person-srcRate ${PERSON_S} -p-auction-source ${AUCTION_P} -p-person-source ${PERSON_P} -p-join ${JOIN_P} &
}

function runSys() {
    python3 -c 'SystemMonitor.py' $1
    echo "INFO:  python3 -c 'SystemMonitor.py' $1"
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

  runSys $3

  SCRIPTS_RUNTIME=`expr ${runtime} - 50 + 10`
  python3 -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'
  stopFlink
}

test() {
#  configApp
  mvRocksdbLog
}

# query(1) mem(2) controller(3) group(4) source(5) try(6) interval(7) winSize(8)
init $1 $2
run_one_exp $3
#test
