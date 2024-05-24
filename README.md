# KV Store

Raft KV store

## Background

Design and implement a distributed k-v storage system based on Go language

## Design

## Todo list

- [x] The TCP port handles client store/read commands
- [x] Support RDB backup
- [x] AOF Backup
~~- [ ] RESP3 protocol implement: https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md~~
~~- [ ] Support Redis Client connect~~
- [x] Support more Redis commands
- [x] Support more Redis data structures
- [ ] Raft algorithm is used to implement fault tolerance

## Reference

- Differences between Redis and Raft: https://wenfh2020.com/2020/10/01/redis-raft/
- MIT 6.824 Distributed System: https://csdiy.wiki/%E5%B9%B6%E8%A1%8C%E4%B8%8E%E5%88%86%E5%B8%83%E5%BC%8F%E7%B3%BB%E7%BB%9F/MIT6.824/#_2