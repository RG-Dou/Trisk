# by default

start() {
#  bash nexmark-run.sh q20 ElasticMemoryManager true 400 $1 1.0 $2 $3 $4
  bash nexmark-run.sh q20 TestInitMemoryManager true 400 $1 1.0 $2 $3 $4
}

caches="300"
for i in {1..1}; do
  for cache in $caches; do
     start ${i} $cache "LRU" "CacheMissEqn"
	done
done
