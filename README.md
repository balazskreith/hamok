!UNDER DEVELOPMENT!
---

Hamok
---
Light and convenient library to create a distributed in-memory object storage grid for java.

## Quick Start
```java
// construct the endpoint of the storage grid
var server_1 = StorageGrid.builder().build();
var server_2 = StorageGrid.builder().build();

// connect the constructed endpoint to each other
server_1.transport().sender().subscribe(server_2.transport().receiver())
server_2.transport().sender().subscribe(server_1.transport().receiver())

// create a storage automatically replicates itself through the grid
var replicatedStorage_1 = server_1.<Integer, String>replicatedStorage().setStorageId("my-distributed-storage").build();
var replicatedStorage_2 = server_2.<Integer, String>replicatedStorage().setStorageId("my-distributed-storage").build();

replicatedStorage_1.set(1, "one");
System.out.println(replicatedStorage_2.get(1));
```

Where `server_1` and `server_2` are created in two different server instances, and the transport is handled by the servers to each other.

## Table of Contents
* []()

## Concept

Horizontal scalability becomes a standard requirement of designing any services today.
Services store objects in the local-memory of the server instance they are running on, but they 
often require sharing ephemeral objects with each other. 
For example when two or more clients sending measurements to two server instances, 
at some point one instance might need to query the number of total clients, 
or the last measurement value of a specific client received by another instance.
This can only be possible if the objects stored by the instances are shared. 
The basic need to share objects between services should not require to add a database 
or any additional dependency to your application. Here comes Hamok into the picture.

Hamok is designed to be a lightweight, distributed object storage with minimal effort to create  
and maximal flexibility to utilize. It includes the necessary logic to 
embed the library and use shared object storages, meanwhile it relies on 
the application developer to manage the cluster of the service.

## Storages

Storages are key, value object maps in hamok, where each storage provides the following operations:
insert, get, set, delete, clear.  

The specific implementation of the storage differs based on the underlying representations.

### Built-in storages

Before continuing with the storage grid it is important to take a look of the built-in storage types.

#### Memory Storage

#### Time limited memory storage

#### Concurrent Memory Storage

#### Concurrent time limited memory storage

#### Custom storage example

## Storage grids

Storage grids are the endpoints of members in hamok. A storage grid 
provide transport to communicate with other members and builders to create different 
type of distributed storages.

### Federated Storage

### Separated Storage

### Replicated Storage

## Collections

## Developer Manual

## Contribution

## License
