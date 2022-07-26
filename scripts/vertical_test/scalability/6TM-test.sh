# by default

PP=24
key_size_nex=300000
state_size_DE=600000
# test
#bash nexmark-run.sh q4 8 350 100000 100000 TestInitMemoryManager true 500 1 false

function testInitMem() {
#  query (1), parallelism (2)，total memory (3), state_size (4), key_size(5), controller (6), group (7), source_rate (8), try_counter (9), input_spy(10)
  bash nexmark-run.sh q4 ${PP} 1000 100000 ${key_size_nex} TestInitMemoryManager true 500 1 false
  bash nexmark-run.sh q20 ${PP} 600 100000 ${key_size_nex} TestInitMemoryManager true 500 1 false

# parallelism (1)，total memory (2), state_size(3), controller (4), group (5), source_rate (6), try_counter (7)
  bash lr-run.sh ${PP} 900 ${state_size_DE} TestInitMemoryManager true 500 1 false
}

function testInputRate() {
#  query (1), parallelism (2)，total memory (3), state_size (4), key_size(5), controller (6), group (7), source_rate (8), try_counter (9), input_spy(10)
  bash nexmark-run.sh q4 ${PP} 600 100000 ${key_size_nex} BlankController true 200 1 true
  bash nexmark-run.sh q20 ${PP} 400 100000 ${key_size_nex} BlankController true 200 1 true

# parallelism (1)，total memory (2), state_size(3), controller (4), group (5), source_rate (6), try_counter (7)
  bash lr-run.sh ${PP} 600 ${state_size_DE} BlankController true 200 1 true
}

function q4run() {
  rates="380 390 400 410 420 430 440"
  for i in {1..2}; do
    for rate in $rates; do
          bash nexmark-run.sh q4 ${PP} 600 100000 ${key_size_nex} BlankController true ${rate} ${i} false
          bash nexmark-run.sh q4 ${PP} 600 100000 ${key_size_nex} ElasticMemoryManager false ${rate} ${i} false
    done
  done
}

function q20run() {
  rates="350 360 370 380 390 400 410"
  for i in {1..2}; do
    for rate in $rates; do
          bash nexmark-run.sh q20 ${PP} 400 100000 ${key_size_nex} BlankController true ${rate} ${i} false
          bash nexmark-run.sh q20 ${PP} 400 100000 ${key_size_nex} ElasticMemoryManager true ${rate} ${i} false
    done
  done
}

function DErun() {
#  rates="380 390 400 410 420 430 440"
  for i in {1..2}; do
    for rate in $rates; do
          bash lr-run.sh ${PP} 600 ${state_size_DE} BlankController true ${rate} ${i} false
          bash lr-run.sh ${PP} 600 ${state_size_DE} ElasticMemoryManager true ${rate} ${i} false
    done
  done
}

#testInitMem
testInputRate
#q4run
#q20run
#DErun
