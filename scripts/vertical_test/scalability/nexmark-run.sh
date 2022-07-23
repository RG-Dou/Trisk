#!/bin/bash
ROOT="$(dirname $(dirname $(dirname $(pwd))))"
FLINK_DIR="$ROOT/Trisk-on-Flink/build-target/"
FLINK_APP_DIR="$ROOT/examples/"
JAR=${FLINK_APP_DIR}$"target/testbed-1.0-SNAPSHOT.jar"
### ### ###  		   ### ### ###

### ### ### INITIALIZATION ### ### ###

### ### ###  		   ### ### ###

# query (1), parallelism (2)，total memory (3), state_size (4), key_size(5), controller (6), group (7), source_rate (8), try_counter (9), input_spy(10)
init() {
  # app level
  DATA_ROOT="/data/EMM_data"
  LATENCY_DIR="${DATA_ROOT}/data/trisk/"
  ### paths configuration ###
  FLINK=$FLINK_DIR$"bin/flink"
  readonly SAVEPOINT_PATH="/home/drg/projects/work3/temp/"
  if [[ "$1" = "q3" ]]; then
    JOB="Nexmark.queries.Query3Stateful"
  elif [[ "$1" = "q3window" ]]; then
    JOB="Nexmark.queries.Query3StatefulWindow"
  elif [[ "$1" = "q5" ]]; then
    JOB="Nexmark.queries.Query5Keyed"
  elif [[ "$1" = "q8" ]]; then
    JOB="Nexmark.queries.Query8Keyed"
  elif [[ "$1" = "q20" ]]; then
    JOB="Nexmark.queries.Query20"
  elif [[ "$1" = "q4" ]]; then
    JOB="Nexmark.queries.Query4"
  fi
  EXP_NAME="nexmark-$1"
  slaves="camel"

  AUCTION_S=0
  PERSON_S=3000
  BID_S=$8
  STATE_SIZE=$4
  KEY_SIZE=$5
#  STATE_SIZE=100000
#  KEY_SIZE=50000
  SKEWNESS=1

  PP=$2
  AUCTION_P=${PP}
  PERSON_P=${PP}
  BID_P=${PP}
  JOIN_P=${PP}
  WIN_P=${PP}
  FILTER_P=${PP}

  runtime=1200
  totalCachePerTM=$3
  Controller=$6
  Group=$7
  Try=$9
  SPY=${10}

  SUB_DIR1=$PP+$totalCachePerTM
  SUB_DIR2=$BID_S+$Controller+$Group+$Try

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
#    sed -ri "s|(state.backend.rocksdb.block.cache-size: )[0-9]*|state.backend.rocksdb.block.cache-size: $totalCachePerTM|" ${FLINK_DIR}conf/flink-conf.yaml
#    sed -ri "s|(taskmanager.memory.managed.fraction: 0.)[0-9]*|taskmanager.memory.managed.fraction: 0.$totalCachePerTM|" ${FLINK_DIR}conf/flink-conf.yaml
    sed -ri "s|(trisk.taskmanager.managed_memory: )[0-9]*|trisk.taskmanager.managed_memory: $totalCachePerTM|" ${FLINK_DIR}conf/flink-conf.yaml
#    sed -i "s/^\(trisk.simple_test: \)\(true\|false\)/\1${simpleTest}/"  ${FLINK_DIR}conf/flink-conf.yaml
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
#    mv ${ROCKSDB_LOG_DIR}* ${DATA_DIR}/${totalCachePerTM}/${simpleTest}
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
  mv ${FLINK_DIR}log/* ${DATA_DIR}/${SUB_DIR1}/${SUB_DIR2}
  mv ${LATENCY_DIR}* ${DATA_DIR}/${SUB_DIR1}/${SUB_DIR2}
  rm -rf /tmp/flink*
  rm ${FLINK_DIR}log/*
  for slave in ${slaves}; do
    scp -r ${slave}:${FLINK_DIR}log/* ${DATA_DIR}/${SUB_DIR1}/${SUB_DIR2}
    scp -r ${slave}:${LATENCY_DIR}* ${DATA_DIR}/${SUB_DIR1}/${SUB_DIR2}
    ssh ${slave} 'rm -rf ${slave}:${FLINK_DIR}log/*'
    ssh ${slave} 'rm -rf ${LATENCY_DIR}*'
    ssh ${slave} 'rm -rf /tmp/flink*'
  done
}

function cleanRocksdbLog() {
    rm -rf ${ROCKSDB_LOG_DIR}*
    rm -rf ${ROCKSDB_CHECKPOINT}*
    for slave in ${slaves}; do
      ssh ${slave} 'rm -rf ${ROCKSDB_LOG_DIR}*'
      ssh ${slave} 'rm -rf ${ROCKSDB_CHECKPOINT}*'
    done
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
  echo "INFO: $FLINK run -c ${JOB} ${JAR} -auction-srcRate ${AUCTION_S} -person-srcRate ${PERSON_S} -bid-srcRate ${BID_S} -p-auction-source ${AUCTION_P} -p-person-source ${PERSON_P} -p-bid-source ${BID_P} -p-join ${JOIN_P} -p-window ${WIN_P} -p-filter ${FILTER_P} -state-size ${STATE_SIZE} -keys ${KEY_SIZE} -group-all ${Group} -skewness ${SKEWNESS} --input-spy ${SPY} &"
  rm nohup.out
  nohup $FLINK run -c ${JOB} ${JAR} -auction-srcRate ${AUCTION_S} -person-srcRate ${PERSON_S} -bid-srcRate ${BID_S} -p-auction-source ${AUCTION_P} -p-person-source ${PERSON_P} -p-bid-source ${BID_P} -p-join ${JOIN_P} -p-window ${WIN_P} -p-filter ${FILTER_P} -state-size ${STATE_SIZE} -keys ${KEY_SIZE} -group-all ${Group} -skewness ${SKEWNESS} --input-spy ${SPY} &
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

# query (1), parallelism (2)，total memory (3), state_size (4), key_size(5), controller (6), group (7), source_rate (8), try_counter (9), input_spy(10)
init $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10}
run_one_exp
#test
