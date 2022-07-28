# by default

# test
#bash nexmark-run.sh q4 8 350 100000 100000 TestInitMemoryManager true 500 1 false
function testInitMem() {
  bash nexmark-run.sh q4 500 TestInitMemoryManager true 500 1 100 1
  bash nexmark-run.sh q20 300 TestInitMemoryManager true 500 1 100 1

  bash lr-run.sh ${PP} 300 TestInitMemoryManager true 500 1 100
}

function testInputRate() {
# query(1) memory(2) controller(3) group(4) source(5) try(6) interval(7) winSize(8)

  rates="500"
  for rate in $rates; do
    bash nexmark-run.sh q4 700 BlankController true ${rate} 1 100 1
  done

  rates="500"
  for rate in $rates; do
    bash nexmark-run.sh q20 300 BlankController true ${rate} 1 100 1
  done

# memory (1) controller(2) group(3) source(4) try(5) interval(6)
  rates="1000 2000"
  for rate in $rates; do
    bash lr-run.sh 500 BlankController true ${rate} 1 100
  done
}

function q4run() {
  rates="1000"
  winSizes="1 5 10 20"
  for i in {1..2}; do
    for winSize in $winSizes; do
      for rate in $rates; do
            bash nexmark-run.sh q4 700 BlankController true ${rate} ${i} 100 ${winSize}
            bash nexmark-run.sh q4 700 ElasticMemoryManager false ${rate} ${i} 100 ${winSize}
      done
    done
  done
}

function q20run() {
  rates="1000"
  intervals="10 100 1000 5000"
  for i in {1..2}; do
    for interval in ${intervals}; do
      for rate in $rates; do
            bash nexmark-run.sh q20 300 BlankController true ${rate} ${i} ${interval} 1
            bash nexmark-run.sh q20 300 ElasticMemoryManager true ${rate} ${i} ${interval} 1
      done
    done
  done
}

function DErun() {
  rates="1000"
  intervals="10 100 1000 5000"
  for i in {1..2}; do
    for interval in ${intervals}; do
      for rate in $rates; do
            bash lr-run.sh 300 BlankController true ${rate} ${i} ${interval}
            bash lr-run.sh 300 ElasticMemoryManager true ${rate} ${i} ${interval}
      done
    done
  done
}

# query(1) controller(2) group(3) source(4) try(5) interval(6) winSize(7)
testInitMem
#testInputRate
#q4run
#q20run
#DErun
