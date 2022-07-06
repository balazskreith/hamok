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
* [Concept](#concept)
* [Storages](#storages)
* [Storage Grids](#storage-grids)
* 

## Concept

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

Before we go to the distributed storage grids, let's clarify what a storage means in hamok.
Storage in hamok is a key, value object map providing the following basic operations:
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

Storage grids are the entry points for distributed storages.

Hamok does not provide locks as part of the distributed storages. As strange as it sounds, 
it attempts to address the problem of distributed concurrent modifications
by relying on the developer to choose the right concept for storing objects for different scenarios.
There are three type of distributed storages in hamok: Replicated, Separated, and Federated.
Each storage type is developed for different scenarios, and the scenarios are detailed in
storage descriptions.

Hamok uses Raft as a consensus algorithm. Raft is combined with a so-called
discovery protocol in hamok, and the combined package called raccoons.

### Create Storage Grids

### Raft + Discovery (Raccoon)

Raccoon is a combination of [Raft](https://raft.github.io/) and the hamok discovery protocol.
Each StorageGrid craetes a raccoon internally. 
If StorageGrid auto-discovery is enabled and StorageGrid transports are connected, 
Raccoons discovering each other by sending hello messages. When a cluster is formed, 
raccoons elect a leader based on raft. The elected leader ensures consistency in the cluster 
for storages requires it (i.e.: ReplicatedStorage).

### Separated Storage

An object in the storage is mutated by only one server instance.
Every server instance can access to every object by key,
but only one instance modifies one object belongs to a specific key.
The instance modifies the object does not have to store it locally.

For example, a client application collect the temperature for a given 
coordinates and send it to service scale horizontally and share 
the storage identified as temperatures amongst the instances.
The key of the measurement is a geographical coordinate, and the value 
is the measured temperature.

```java
// after you defined your storage grid
var keyCodec = Codec.create(GeoLocation::toString, GeoLocation::fromString);
var valueCodec = Codec.create(d -> d.toString(), Double::parseDouble);
var temperatures = storageGrid.<GeoLocation, Double>separatedStorage()
        .setStorageId("temperatures")
        .setKeyCodec(() -> keyCodec)
        .setValueCodecSupplier(() -> valueCodec)
        .build();

// ... client received measurements
temperatures.set(measurement.location, measurement.temperature);
```

NOTE: GeoLocation is a custom class 

### Federated Storage

An object in the storage is a combination of multiple subpart stored in 
one or more server instances. Each time an object is accessed by a key at any server 
instance, the server instance requests the subpart from all other 
server instances and provide the result as the combination of the 
responses.

Keeping the 

A globally available object in the storage for a key is a combination of 
multiple locally altered objects by several instances and the result is the 
combination of all instance locally stored objects for a given key. 

### Replicated Storage



### CAP analysis

Consistency and Availability: For federated and separated storages the consistency and 
availability rely on the developer creates the storage. In case of 
replicated storage, consistency is ensured by raft.

Partition tolerance: In case of message dropped by the network or any request failure, 
the StorageGrid retries it. The number of retries is configurable when the 
StorageGrid is built.

## Codecs

## Transports

## Collections

## Developer Manual

## Contribution

## License



## Sentence purgatory

In case you have a service collect measurements from client
applications, and the measurements are separated and should be shared amongst all
instances of the service, Separated Storage can be choice. It
makes the collected measurements available amongst all server instances,
meanwhile only one client provides the measurements mutate the storage
for a given key (the key can be for example the client identifier).