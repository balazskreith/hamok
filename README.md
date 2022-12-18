Hamok
---
Light and convenient library to create a distributed in-memory object storage grid for java.

## Quick Start

Install

```
implementation group: 'io.github.balazskreith.hamok', name: 'hamok-java-core', version: '1.0.0'
```

Usage

```java
// construct the storage grid on two servers
var server_1 = StorageGrid.builder().build();
var server_2 = StorageGrid.builder().build();

// connect the storage grids of server 1 and 2
server_1.transport().sender().subscribe(server_2.transport().receiver())
server_2.transport().sender().subscribe(server_1.transport().receiver())

// create a storage automatically replicates itself through the grid
var replicatedStorage_1 = server_1.<Integer, String>replicatedStorage()
        .setStorageId("my-distributed-storage")
        .setCodec()
        .build();

var replicatedStorage_2 = server_2.<Integer, String>replicatedStorage()
        .setStorageId("my-distributed-storage")
        .setCodec()
        .build();

replicatedStorage_1.set(1, "one");
System.out.println(replicatedStorage_2.get(1));
```

Where `server_1` and `server_2` are created on two different server instances.

## Table of Contents
* [Intro](#intro)
* [Concepts](#concepts)
* [Storages and Collections](#storages)
* [Storage Grids](#storage-grids)
* 

## Intro

Horizontal scalability becomes a standard requirement of designing services.
An instance of a service stores objects in the local-memory of the server
it is running on, but it is often required to share ephemeral locally stored objects with 
another service instance.

For example when two or more clients sending measurements to two instances of the same service,
at some point one instance might need to query the number of total clients,
or the last measurement value of a specific client received by another instance.
This can only be possible if the objects stored by the instances are shared in some way.

The basic need to share objects between services may not require to add a
database as an additional dependency. Here comes Hamok into the picture.

Hamok is designed to be a lightweight, distributed object storage with minimal
effort to create and maximal flexibility to utilize. It includes the necessary logic to
embed the library and use shared object storages, meanwhile it relies on
the application developer to manage the cluster of the service and the transport between the instances.

## Concepts

Hamok is designed to be a library providing object storages (i.e.: Map, List, etc.) for 
applications. The provided object storages can be linked together if they are 
created in the same grid. The so-called StorageGrid automatically maintain 
communications with the linked storages.

### Storages

Storage in hamok is a key, value object map. Storages have the following base operations:
insert, set, get, delete, clear, evict, restore. The batched version of the listed operations
(insertAll, setAll, getAll, deleteAll, evictAll, restoreAll) are also available, and 
every storage provide events to notify subscribers when storage entries are created or altered.

The concrete implementation of a hamok storage varies based on requirements.
A concrete storage can be a simple memory storage, a concurrent time limited memory storage,
or a client library of a database representing a table as a storage.

### Accessibility and distribution

When it comes to access to shared objects in distributed environments 
one could use distributed locks. However, hamok by design prefers avoiding 
distributed locks. Hamok relies on developers to design their shared storages considering 
the nature of mutating the stored entry. There are two concepts exists in hamok to access, and store entries:
separation and replication. 

### Separation

Entries in separated storages are distributed by the key of the entry. The base assumption for a separated storage that 
a value belongs to a key is requested to altered by one and only one client at a time.
For example this is the case if a client provides measurements 
to a cluster of servers and the identifier of the client is unique. Client joins to the cluster and informs the 
instance about the update of its state. The actual instance performs the update locally if the key is stored 
on the connected instance or request to perform on another instance if it does not find it. 
Any server instance can access to any client provided measurements, 
but only one server instance (which stores the entry) mutates the corresponded value.

### Replication

Entries in replicated storages are distributed via a replication process. The replication process 
ensures statements are executed in the same order on every instance, therefore every instance 
have the exact same replication of a storage. In replication many instances can 
alter the entry belongs to the same key. 
For example a configuration for a service could be replicated amongst the instances. Whenever 
one instance change the configuration it is automatically replicated through all.

### Transports

The concept of transport objects from one instance to another is out of the scope of hamok.
Practically it means that the application developer is responsible for connectivity between 
the server instances. Once a connectivity is ensured the storage grid takes care the rest. 
Example to implement transports between instances in different environment can be found [here]().

## User Manual

### Storages

Let's create a simple storage can be accessed though multiple thread and stores key, value pairs in the memory.
```java
var storage = MemoryStorage.<Integer, String>builder().setConcurrent(true).build();
```

#### Operations

**insert, insertAll:** creates new entry in the storage, if the corresponded key(s) does not have an associated value(s).

```java
// create your storage
var key = 1;
var value = "myValue";
var alreadyInsertedValue = storage.insert(key, value);
if (alreadyInsertedValue != null) {
    // value for key is already inserted
}

var alreadyInsertedEntries = storage.insertAll(Map.of(key, value));
if (0 < alreadyInsertedEntries.size()) {
    // value for key is already inserted
}
```

**set, setAll:** creates or update entries in the storage
```java
// create your storage
var key = 1;
var value = "myValue";
var oldValue = storage.set(key, value);
if (oldValue != null) {
    // key had a previously associated value
}

var oldEntries = storage.setAll(Map.of(key, value));
if (0 < oldEntries.size()) {
    // key had a previously associated value
}
```


**delete, deleteAll:** delete entries from storage associated to the given keys. the methods returns information about the result of the operation.
```java
// create your storage
var key = 1;

if (storage.delete(key)) {
    // key was present in the storage and it was successfully deleted
}

var removedKeys = storage.deleteAll(Set.of(key));
if (removedKeys.contains(key)) {
    // key was present in the storage and it was successfully deleted
}
```

**evict, evictAll:** evicts entries from the storage associated to the given keys. the method does not give any response to the operation success, 
but trigger event of evicted entry if an entry is evicted. It is mainly used to manage backups.
```java
// create your storage
var key = 1;
storage.evict(key);
storage.evictAll(Set.of(key));
```

**restore, restoreAll:** add entries to the storage if they have not been added before. similar to the insert, but in this case 
an exception is thrown if the entry already exists. It is mainly used to manage backups.

```java
// create your storage
var key = 1;
var value = "value";
storage.restore(key, value);
storage.evictAll(Map.of(key, value));
```

#### Events

**Created Entry**: Triggered if a key, has not been existed in the storage is associated to a value

```java
// ... create storage, and logger
storage.events().createdEntry().subscribe(event -> {
        logger.info("An entry is created. key: {}, value, timestamp: {}",
        event.getKey(),
        event.getOldValue(),
        event.getNewValue(),
        event.getTimestamp());
});
```

**Updated Entry**: Triggered if a key already existing in the storage associated to a new value 

```java
// ... create storage, and logger
storage.events().updatedEntry().subscribe(event -> {
    logger.info("An entry is updated. key: {}, old value: {}, new value: {} timestamp: {}", 
        event.getKey(), 
        event.getOldValue(),  
        event.getNewValue(), 
        event.getTimestamp());
});
```

**Deleted Entry**: Triggered if a key existed in the storage deleted by a delete method
```java
storage.events().deletedEntry().subscribe(event -> {
        logger.info("An entry is deleted. key: {}, deleted value: {}, timestamp: {}",
        event.getKey(),
        event.getOldValue(),
        event.getTimestamp());
});
```

**Evicted Entry**: Triggered if a key existed in the storage deleted by a evict method
```java
storage.events().evictedEntry().subscribe(event -> {
        logger.info("An entry is evicted. key: {}, evicted value: {}, timestamp: {}",
        event.getKey(),
        event.getOldValue(),
        event.getTimestamp());
});
```

**Restored Entry**: Triggered if a key has not been existed in the storage is associated to a value by calling the restore method explicitly
```java
storage.events().restoredEntry().subscribe(event ->{
        logger.info("An entry is restored. key: {}, restored value: {}, timestamp: {}",
        event.getKey(),
        event.getNewValue(),
        event.getTimestamp());
});
```

**Closed Storage**: Triggered if the storage is closed
```java
storage.events().closingStorage().subscribe(storageId ->{
        logger.info("Storage {} is closed",storageId);
});
```

##### Batched events

Events can be collected and emitted as batch of events
```java
var collectedEvents = storage.events().collectOn(Schedulers.computation());
collectedEvents.createdEntries().subscribe(events -> {
    
});
collectedEvents.updatedEntries().subscribe(events -> {

});
collectedEvents.deletedEntries().subscribe(events -> {

});
collectedEvents.evictedEntries().subscribe(events -> {

});
collectedEvents.restoredEntries().subscribe(events -> {

});
```

### StorageGrid

Creating distributed storages can be done through storage grids. 

```java
var storageGrid = StorageGrid.builder().build();
var intEncoder = integer -> ByteBuffer.allocate(4).putInt(integer).array();
var intDecoder = bytes -> ByteBuffer.wrap(bytes).getInt();
var storage = storageGrid.<Integer, Integer>createSeparatedStorage()
        .setStorageId("my-separated-storage")
        .setKeyCodec(intEncoder, intDecoder)
        .setValudeCodec(intEncoder, intDecoder)
        .build()
```

The above code snippet creates a storage grid client in the application, and from that 
a separated storage is created serialize and deserialize integers as key and values.

StorageGrids utilizes Raft algorithm for a leader election and provide api endpoints for transport.
If you want to change the default configurations you can do that at building stage:
```java
var storageGrid = StorageGrid.builder()
        
        // Sets the timeout in follower state. if follower does not receive
        // request from a leader it starts an election
        .withFollowerMaxIdleInMs(1500)
        
        // Sets the timeout for leader election
        .withElectionTimeoutInMs(2000)
        // The time interface in ms the leader sends a heartbeat to followers
        .withHeartbeatInMs(100)
        
        // the interval of sending hello messages in follower state
        // to discover other endpoints. Only sending in follower state, 
        // and only until a leader does not sending an endpoint notification message
        .withSendingHelloTimeoutInMs(1500)
        
        // Sets the timeout after a leader claims a follower is not available, 
        // and informs the grid about an endpoint detached.
        .withPeerMaxIdleTimeInMs(5000)
        
        // if a follower cannot be synced up with the grid, it starts a syncronization process 
        // this sets a maximum time to finish it
        .withApplicationCommitIndexSyncTimeoutInMs(30000)
        
        // the expiration time of the Raft logs. If a log expires and a member joins 
        // after the logs are expired the newly joined member has to 
        // request a storage sync. 0 means there is no expiration time
        .withRaftMaxLogRetentionTimeInMs(0)
        
        // Indicate if the StorageGrid should discover remote endpoint for Raft
        // by itself or it is added through calling storageGrid.join / detach endpoint API
        .withAutoDiscovery(true)
        .build();
```

### Create ReplicatedStorage

Replicated storages use the StorageGrid Raft layer to synchronize entries inside the storage.
The upside of using replicated storage that it is ensured that in every replica the operations are executed in exactly the same order, 
hence this is the storage for example where insert operation ensures the distribution for 
the inserted entry without using any distributed locks. 
The downside of using replicated storage is speed and volume. All entries are distributed 
on all instances, and every operation of mutating entries has to be submitted to 
Raft, which takes a while (a couple of heartbeats) to be executed.

```java
var storage = storageGrid.<Integer, String>createReplicatedStorage()
        // Sets the id of the storage
        .setStorageId("my-replicated-storage")
        
        // by default replicate storage creates a concurrent memory storage, 
        // but it is possible to customize it.
        // in this case setStorageId is not necessary
        .setStorage(myStorage)
        
        // sets the key to bytesm and bytes to key functions
        .setKeyCodec(keyEncoder, keyDecoder)
        
        // sets the value to bytes and bytes to values functions
        .setValueCodec(valueEncoder, valueDecoder)

        // sets the maximum number of keys can be transported in one message
        // if this value is higher than 0 then it chunks all requests of operation 
        // pushes more keys or entries than the max allowed one.
        .setMaxMessageKeys(0)

        // sets the maximum number of values can be transported in one message
        // if this value is higher than 0 then it chunks all requests of operation 
        // pushes more values or entries than the max allowed one.
        .setMaxMessageValues(0)
        .build();
```

### Create SeparatedStorage

Separated storages are not using Raft to collect remote entries. If a requested entry 
is not found in a local storage then it requests all remote peers to provide it.
The upside of this storage the size and speed. Operations are executed concurrently on every instances 
and an entry is (in case there is only one client source mutates it) stored on only one instance.
The downside is that in case more than one client source mutates the entry it cannot be ensured 
that the value of the entry will not be inconsistent, stored in many server instances etc.

```java
var storage = storageGrid.<Integer, String>createSeparatedStorage()
        // Sets the id of the storage
        .setStorageId("my-replicated-storage")
        
        // by default replicate storage creates a concurrent memory storage, 
        // but it is possible to customize it.
        // in this case setStorageId is not necessary
        .setStorage(myStorage)
        
        // sets the key to bytesm and bytes to key functions
        .setKeyCodec(keyEncoder, keyDecoder)
        
        // sets the value to bytes and bytes to values functions
        .setValueCodec(valueEncoder, valueDecoder)

        // sets the maximum number of keys can be transported in one message
        // if this value is higher than 0 then it chunks all requests of operation 
        // pushes more keys or entries than the max allowed one.
        .setMaxMessageKeys(0)

        // sets the maximum number of values can be transported in one message
        // if this value is higher than 0 then it chunks all requests of operation 
        // pushes more values or entries than the max allowed one.
        .setMaxMessageValues(0)
        .build();
```

## Maven central

https://central.sonatype.dev/artifact/io.github.balazskreith.hamok/hamok-java-core/0.9.2-beta/versions

## Contribution

## License
