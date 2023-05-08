# Read Benchmarks

From a c7g.xlarge machine in the same region:

fetch_random on ~1 MB blocks (between 112-1700 KB, depending on compresison ratio):
```
2023-05-08T16:34:36.944068Z DEBUG fetch_random: fetches=10000 mean=32796.0us p99=80719.4us
```

If we simulate a 50% block cache hit ratio:
```
2023-05-08T16:39:39.602732Z DEBUG fetch_random: fetches=10000 mean=15727.0us p99=64820.7us
```

we get roughly linear speedup — fetching from S3 is >99% of the observable latency.

As expected, if we simulate a 100% block cache hit rate, performance is very good:
```
2023-05-08T16:28:35.349016Z DEBUG fetch_random: fetches=1000000 mean=2.7us p99=5.4us
```