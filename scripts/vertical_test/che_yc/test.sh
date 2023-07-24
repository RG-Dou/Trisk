# by default
caches="500"
for i in {1..1}; do
  for cache in $caches; do
     bash nexmark-run.sh ${i} $cache "clock"
	done
done
