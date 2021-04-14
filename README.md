## PM-ART: Durable Linearizable Adaptive Radix Tree

### Branches:
- master is our correct implementation.
- no_nvm_mgr allocates nodes in dram and use for some test.
- nvm_mgr is the development branch that we develop the correct nvm memory management.
- pmdk_mgr is using pmdk to test other indexes.

## Build & Run

#### Build

```
$ mkdir build
$ cd build
$ cmake ..
$ make -j
```

#### Run

```
$ ./benchmark
```