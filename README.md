# Multithreaded HTTP Server (SFF)

## Description
HTTP server implemented in C using:
- POSIX threads
- Thread pool
- Bounded request queue
- Smallest File First (SFF) scheduling

## Architecture
Client → Accept Thread → Priority Queue → Worker Threads → Response

## Features
- Concurrent request handling
- SFF scheduling (small files prioritized)
- Logging of request order and processing
- Benchmarking support

## Run
gcc -pthread src/phase3.c -o server
./server 4 10

## Benchmark
./scripts/bench.sh 200 20 http://localhost:8080 small.html medium.html large.html
