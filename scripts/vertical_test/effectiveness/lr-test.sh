# by default
rates="2000"
skews="0.8 1.0 1.2"
for i in {1..10}; do
  for rate in $rates; do
    for skew in $skews; do
    # bash lr-run.sh BlankController false ${rate} ${i} ${skew}
    #	bash lr-run.sh BlankController true ${rate} ${i} ${skew}
      bash lr-run.sh ElasticMemoryManager false ${rate} ${i} ${skew}
	  done
	done
done
