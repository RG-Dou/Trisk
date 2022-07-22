# by default

# test
bash nexmark-run.sh q4 8 512 BlankController false 500 1

APP="q4 q20 Daily"

# test memory
bash nexmark-run.sh q4 8



#rates="2000"
#for i in {1..10}; do
#  for rate in $rates; do
#	# bash nexmark-run.sh q4 BlankController false ${rate} ${i}
#  #	bash nexmark-run.sh q4 BlankController true ${rate} ${i}
#	  bash nexmark-run.sh q4 ElasticMemoryManager false ${rate} ${i}
#	done
#done
