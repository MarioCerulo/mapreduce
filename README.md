# MapReduce

Implementation of the MapReduce programming model, based on [MapReduce: Simplified Data Processing on Large Clusters](https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/), written in Go

## Overview

The framework separates computation from coordination. 
Users implement two functions:

- `Map(key, value string) []KeyValue`: processes an input chunk and emits intermediate pairs
- `Reduce(key string, values []string) string`: aggregates all values for a key into a single result

The coordinator handles task assignment and worker coordination.
Each worker is responsible for the entire task lifecycle, from reading the input files, to executing the task, to writing the result back to storage

## Running

Start the coordinator:
```bash
just coordinator <reducers>
```

Start one or more workers:
```bash
just worker
```

### Requirements
- go >= 1.26.1
