# by default
rates="2000"
for i in {1..10}; do
  for rate in $rates; do
	# bash lr-run.sh BlankController false ${rate} ${i}
  #	bash lr-run.sh BlankController true ${rate} ${i}
	  bash lr-run.sh ElasticMemoryManager false ${rate} ${i}
	done
done
