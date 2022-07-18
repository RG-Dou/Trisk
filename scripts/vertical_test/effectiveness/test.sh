# by default
rates="2000"
for i in {1..10}; do
  for rate in $rates; do
	# bash nexmark-run.sh q4 BlankController false ${rate} ${i}
  #	bash nexmark-run.sh q4 BlankController true ${rate} ${i}
	  bash nexmark-run.sh q4 ElasticMemoryManager false ${rate} ${i}
	done
done
