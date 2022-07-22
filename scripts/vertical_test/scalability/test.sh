# by default

# test
#bash nexmark-run.sh q4 8 350 100000 100000 TestInitMemoryManager true 500 1 false

function testInitMem() {
#  query (1), parallelism (2)，total memory (3), state_size (4), key_size(5), controller (6), group (7), source_rate (8), try_counter (9), input_spy(10)
  bash nexmark-run.sh q4 8 350 100000 100000 TestInitMemoryManager true 500 1 false
  bash nexmark-run.sh q20 8 350 100000 100000 TestInitMemoryManager true 500 1 false

# parallelism (1)，total memory (2), state_size(3), controller (4), group (5), source_rate (6), try_counter (7)
  bash lr-run.sh 8 350 200000 TestInitMemoryManager true 500 1 false
}

function testInputRate() {
#  query (1), parallelism (2)，total memory (3), state_size (4), key_size(5), controller (6), group (7), source_rate (8), try_counter (9), input_spy(10)
  bash nexmark-run.sh q4 8 600 100000 100000 BlankController true 500 1 true
  bash nexmark-run.sh q20 8 600 100000 100000 BlankController true 500 1 true

# parallelism (1)，total memory (2), state_size(3), controller (4), group (5), source_rate (6), try_counter (7)
  bash lr-run.sh 8 600 200000 BlankController true 500 1 true
}

testInitMem
testInputRate


#rates="2000"
#for i in {1..10}; do
#  for rate in $rates; do
#	# bash nexmark-run.sh q4 BlankController false ${rate} ${i}
#  #	bash nexmark-run.sh q4 BlankController true ${rate} ${i}
#	  bash nexmark-run.sh q4 ElasticMemoryManager false ${rate} ${i}
#	done
#done
