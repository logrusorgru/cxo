CXDS implementations
====================

- `badger` based on [Badger DB](github.com/dgraph-io/badger)
- `bolt` based on [Bolt DB](github.com/boltdb/bolt)
- `memory` based on golang map
- `readis` based on [Redis](redis.io) using [radix](github.com/mediocregopher/radix.v3)

For spinning disk test cases take:

| DB engine  | Time | Note |
| ------------- | ------------- |
| `badger`  | 20.750s  | tested on spinning discks, but the badger designed for SSD |
| `bolt`  | 7.162s  | designed for spinning discks and tested on them |
| `memory`  | 0.010s  | in memory |
| `redis`  | 1.165s  | - |

Detailed benchmarks coming soon.
