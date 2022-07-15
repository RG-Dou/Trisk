# by default
rates="2100"
for rate in $rates; do
#	bash nexmark-run.sh q4 BlankController false ${rate}
#	bash nexmark-run.sh q4 BlankController true ${rate}
	bash nexmark-run.sh q4 ElasticMemoryManager false ${rate}
done
