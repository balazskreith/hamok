## Concept

Light and reactive based implementation 
for applications to virtualize the underlying storage system. 

## In Memory Database Grid (IMDG)

StorageGrid

## Custom Storages


### SeparatedStorageGrid
 
Only one client writes an entry, others read.  

In this case the storage is sliced and only one writes and updates an entry.
One backup for every entry.
if any node left, other nodes take the backup of the fallen node backupStorage,
which should automatically redistribute to backup the new owned entries.

## FederatedStorageGrid

every node writes locally and merge globally.

in this case every node writes into its own storage, but
when item is got it is from every node and the result is "merged".

### ReplicatedStorageGrid

We know what is it.

## FAQ

### How can I connect collections accross the network?

Well, first you need to embed vcollection into your project, and then you connect it to your existing implementation of 
your network transport communication, or you create one, and that is how! 
The responsibility of vcollection ends at the moment of transport in this case.
This is a library created for separating the logic between collections and storages. 
The obvious advantage is that you can make collections scalable as you need: store it in file or in a memory, 
make a cached storage without touches any of the code accessing to the collection, or, spread them accross the network. 
To make a horizontally scalable collection on the network vcollection provides you the storage types you might want to use, but again it does not implement for 
you a single line of code how to communicate on the network, or what kind of serialization codec you want to use. 
As it is written before, you can connect it to your existing solution, or you can create your own network communication protocol through for instance netty.

### Can I build storages from configuration files, like yaml?

If you implement such kind of builder, then yes. It was on the table to add it to the library, but pushed out for two reasons:
one it would add extra library dependency, and two (this is the main reason) because vcollection use generic types for the storages, 
and its getting way too complicated to make it a generally configurable way to create factories to create generic types to create storages to create collections.
So initially it was planned to make a beautiful configuration possibilities through yaml and json and everything is just built up 
when the application starts, but then I realized that I started to derail from the original goal. The goal was/is to make a 
separation between storage and collection and not to make a building system for configurations. 



