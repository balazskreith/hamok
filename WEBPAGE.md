Hamok
---
Light and convenient library to create a distributed in-memory object storage grid for java.


## Quick start

#### Java

```java

```

#### Typescript

```typescript

```

## Basic concept

Horizontal scalability becomes a standard requirement of designing services today.
Service stores objects in the local-memory of the server instance they are running on, but they 
often require sharing objects with each other for various purposes. 

For example when two or more clients sending measurements to two server instances, 
at some point one instance might need to query the number of total clients, 
or the last measurement value of a specific client received by another instance.
This can only be possible if the objects stored by the instances are shared. 
The basic need to share objects between services should not require to add a database 
or any additional dependency. Here comes Hamok into the picture.

Hamok is designed to be a lightweight, distributed object storage with minimal effort to create  
and maximal flexibility to utilize. It includes the necessary logic to 
embed the library and use shared object storages, meanwhile it relies on 
the application developer to manage the cluster of the service.


## Quick start

#### Java

```java

```

## Storages

Before we go to the distributed storage grids, let's clarify what a storage means in hamok.
Storage in hamok is a key, value object map providing the following operations: 
insert, set, get, delete, clear. Additionally, every storage provide events 
to notify subscribers when a storage is altered or closed.

The concrete implementation of a hamok storage varies based on requirements. 
A concrete storage can be a simple memory storage, a concurrent time limited memory storage, 
or a client library of a database representing a table as a storage.

Using a Storage interface in your application, but changing the actual storage 
instance based on environment (simple memory storage in test, database representation in prod, etc.) 
gives the flexibility for the developers to decompose the service they are developing from the 
underlying representation.

One more note, before we go to the Storage Grid sections. Each storage provides a method 
for batched operations (insertAll, setAll, getAll, deleteAll), which are preferred if performance 
comes into the picture.

## Storage grids

Storage grids are the endpoints of members in hamok for distributed storage systems.
The most important note about the library, that Hamok does not provide locks as part of the 
distributed storages.
As strange as it sounds, it attempts to address the problem of distributed concurrent modifications 
by relying on the developer to choose the right concept for storing objects for various needs.
There are three type of distributed storages in hamok: Replicated, Separated, and Federated.
Each storage type is developed for different scenarios, and the scenarios are detailed in 
storage descriptions.

Hamok uses Raft as a consensus algorithm. Raft is combined with a so-called 
discovery protocol in hamok, and the combined package called raccoons. 
A raccoon object is responsible to keep the cluster up-to-date,

**Scenario 1**: An object in the storage is mutated by only one server instance.
Every server instance can access to every object by key, 
but only one instance modifies one object belongs to a specific key. 
The instance modifies the object does not have to store it locally.

**Scenario 2**: An object in the storage is mutated by several instance
This scenario comes up for example when 

### Raft + Discovery (Raccoon)


### Replicated Storage


### Separated Storage

Separated storages are designed to be set by a single client and get by multiple clients

### Federated Storage

Federated storages are designed to be set by multiple clients and get the result 
as a federated one. The operation to federate the 



## Language bindings

### Java

### Typescript

