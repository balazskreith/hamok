Hamok
---
Light and convenient library to create a distributed object storage.


## Quick start

#### Java

```java

```

#### Typescript

```typescript

```

## Basic concept

Horizontal scalability becomes a standard requirement of designing services today.
A running service stores objects in the local-memory of the server instance 
they are running on, but they often require sharing objects with each 
other for various purposes. 

For example when two or more clients sending measurements to two server instances, 
at some point one instance might need to query the number of total clients, 
or the last measurement value of a specific client received by another instance.
This can only be possible if the objects stored by the instances are shared. 
The basic need to share objects between services may not require to add a 
database as an additional dependency. Here comes Hamok into the picture.

Hamok is designed to be a lightweight, distributed object storage with minimal 
effort to create and maximal flexibility to utilize. It includes the necessary logic to 
embed the library and use shared object storages, meanwhile it relies on 
the application developer to manage the cluster of the service.

## Storages

## Storage grids

### Raft + Discovery (Raccoon)

### Replicated Storage

### Separated Storage

### Federated Storage

## Language bindings

### Java

### Typescript

