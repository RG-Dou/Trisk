# by default

start() {
  bash nexmark-run.sh q20 ElasticMemoryManager true 1000 $1 1.0 $2 $3 $4
}

caches="500"
for i in {1..1}; do
  for cache in $caches; do
     start ${i} $cache "LRU" "CacheMissEqn"
	done
done
