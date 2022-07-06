Hamok
---

Light and convenient library to create a simple storage grid.

## Quick Start
```java
var server_1 = StorageGrid.builder().build();
var server_2 = StorageGrid.builder().build();

server_1.transport().sender().subscribe(server_2.transport().receiver())
server_2.transport().sender().subscribe(server_1.transport().receiver())

var replicatedStorage_1 = server_1.<Integer, String>replicatedStorage().setStorageId("my-distributed-storage").build();
var replicatedStorage_2 = server_2.<Integer, String>replicatedStorage().setStorageId("my-distributed-storage").build();

replicatedStorage_1.set(1, "one");
System.out.println(replicatedStorage_2.get(1));
```

Where `server_1` and `server_2` are created in two different server applications, and the transport is handled by the servers with each other.

## Table of Contents
* []()

## Storages

### Built-in storages

### Custom storages

## Storage Grid

## Reactive Events

## Developer Manual

## Contribution

## License
