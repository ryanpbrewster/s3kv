# Read Benchmarks

From a c7g.xlarge machine in the same region:

fetch_random on ~1 MB blocks (between 112-1700 KB, depending on compresison ratio):
```
2023-05-05T21:50:50.909048Z DEBUG fetch_random: fetches=10000 mean=30733.28us p99=74133us
```

If we simulate a 50% block cache hit ratio:
```
2023-05-05T22:01:15.246500Z DEBUG fetch_random: fetches=5000 mean=15470.14us p99=69138us
```

we get roughly linear speedup — fetching from S3 is effectively 100% of the observable latency.

As expected, if we simulate a 100% block cache hit rate, performance is very good:
```
2023-05-05T22:11:04.745660Z DEBUG fetch_random: fetches=100000 mean=6.962us p99=6.640us
```