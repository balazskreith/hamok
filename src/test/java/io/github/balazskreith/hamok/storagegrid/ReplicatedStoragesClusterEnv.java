package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.UuidTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ReplicatedStoragesClusterEnv {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatedStoragesClusterEnv.class);

    public static final String STORAGE_ID = "Test-Storage-" + UUID.randomUUID().toString().substring(0, 8);
    private StorageGrid euWest;
    private StorageGrid usEast;
    private StorageGrid asEast;
    private ReplicatedStorage<String, Integer> euStorage;
    private ReplicatedStorage<String, Integer> usStorage;
    private ReplicatedStorage<String, Integer> asStorage;
    private StorageGridRouter router = new StorageGridRouter();
    private BinaryOperator<Integer> mergeOp = (itemsFromStockPile1, itemsFromStockPile2) -> itemsFromStockPile1 + itemsFromStockPile2;
    private volatile boolean created = false;
    private int maxRetentionTimeInMs = 0;
    private int maxCollectedStorageEvents = 1;
    private int maxCollectedStorageTimeInMs = 0;
    private boolean autoDiscovery = false;
    private AtomicReference<StorageGrid> leaderGrid = new AtomicReference<>(null);

    public ReplicatedStoragesClusterEnv setMaxRetention(int maxRetentionTimeInMs) {
        this.requireNotCreated();
        this.maxRetentionTimeInMs = maxRetentionTimeInMs;
        return this;
    }

    public ReplicatedStoragesClusterEnv setMaxCollectedStorageEvents(int maxCollectedStorageEvents) {
        this.requireNotCreated();
        this.maxCollectedStorageEvents = maxCollectedStorageEvents;
        return this;
    }

    public ReplicatedStoragesClusterEnv setMaxCollectedStorageTimeInMs(int maxCollectedStorageTimeInMs) {
        this.requireNotCreated();
        this.maxCollectedStorageTimeInMs = maxCollectedStorageTimeInMs;
        return this;
    }

    public ReplicatedStoragesClusterEnv setMergeOp(BinaryOperator<Integer> mergeOp) {
        this.requireNotCreated();
        this.mergeOp = mergeOp;
        return this;
    }

    public ReplicatedStoragesClusterEnv setAutoDiscovery(boolean value) {
        this.requireNotCreated();
        this.autoDiscovery = value;
        return this;
    }

    public void clear() {
        this.euStorage.clear();
        this.usStorage.clear();
    }

    public ReplicatedStoragesClusterEnv create() {
        this.requireNotCreated();
        this.euWest = StorageGrid.builder()
                .withContext("Eu West")
                .withRaftMaxLogRetentionTimeInMs(this.maxRetentionTimeInMs)
                .withAutoDiscovery(this.autoDiscovery)
                .build();
        this.usEast = StorageGrid.builder()
                .withContext("US east")
                .withRaftMaxLogRetentionTimeInMs(this.maxRetentionTimeInMs)
                .withAutoDiscovery(this.autoDiscovery)
                .build();
        this.asEast = StorageGrid.builder()
                .withContext("AS east")
                .withRaftMaxLogRetentionTimeInMs(this.maxRetentionTimeInMs)
                .withAutoDiscovery(this.autoDiscovery)
                .build();

        Function<Integer, byte[]> intEnc = i -> ByteBuffer.allocate(4).putInt(i).array();
        Function<byte[], Integer> intDec = b -> ByteBuffer.wrap(b).getInt();
        Function<String, byte[]> strEnc = s -> s.getBytes();
        Function<byte[], String> strDec = b -> new String(b);

        this.euWest.addRemoteEndpointId(this.usEast.endpoints().getLocalEndpointId());
        this.euWest.addRemoteEndpointId(this.asEast.endpoints().getLocalEndpointId());
        this.usEast.addRemoteEndpointId(this.euWest.endpoints().getLocalEndpointId());
        this.usEast.addRemoteEndpointId(this.asEast.endpoints().getLocalEndpointId());
        this.asEast.addRemoteEndpointId(this.euWest.endpoints().getLocalEndpointId());
        this.asEast.addRemoteEndpointId(this.usEast.endpoints().getLocalEndpointId());


        this.euStorage = euWest.<String, Integer>replicatedStorage()
                .setStorageId(STORAGE_ID)
                .setMaxCollectedStorageEvents(this.maxCollectedStorageEvents)
                .setMaxCollectedStorageTimeInMs(this.maxCollectedStorageTimeInMs)
                .setKeyCodec(strEnc, strDec)
                .setValueCodec(intEnc, intDec)
                .build();

        this.usStorage = usEast.<String, Integer>replicatedStorage()
                .setStorageId(STORAGE_ID)
                .setMaxCollectedStorageEvents(this.maxCollectedStorageEvents)
                .setMaxCollectedStorageTimeInMs(this.maxCollectedStorageTimeInMs)
                .setKeyCodec(strEnc, strDec)
                .setValueCodec(intEnc, intDec)
                .build();

        this.asStorage = asEast.<String, Integer>replicatedStorage()
                .setStorageId(STORAGE_ID)
                .setMaxCollectedStorageEvents(this.maxCollectedStorageEvents)
                .setMaxCollectedStorageTimeInMs(this.maxCollectedStorageTimeInMs)
                .setKeyCodec(strEnc, strDec)
                .setValueCodec(intEnc, intDec)
                .build();

        this.created = true;
        return this;
    }

    public void destroy() {
        this.requireCreated();
        this.router.disable();
        this.euWest.close();
        this.usEast.close();
        this.asEast.close();
    }

    public void await() throws ExecutionException, InterruptedException, TimeoutException {
        this.requireCreated();
        this.await(0);
    }

    public void await(int timeoutInMs) throws ExecutionException, InterruptedException, TimeoutException {
        this.requireCreated();
        var euWestIsReady = new CompletableFuture<UUID>();
        var usEastIsReady = new CompletableFuture<UUID>();
        var asEastIsReady = new CompletableFuture<UUID>();

        euWest.events().joinedRemoteEndpoints().subscribe(euWestIsReady::complete);
        usEast.events().joinedRemoteEndpoints().subscribe(usEastIsReady::complete);
        asEast.events().joinedRemoteEndpoints().subscribe(asEastIsReady::complete);

        euWest.events().changedLeaderId().subscribe(leaderIdHolder -> {
            if (leaderIdHolder.isEmpty()) {
                var currentLeader = this.leaderGrid.get();
                if (currentLeader != null && UuidTools.equals(currentLeader.endpoints().getLocalEndpointId(), euWest.endpoints().getLocalEndpointId())) {
                    this.leaderGrid.set(null);
                }
            } else if (UuidTools.equals(leaderIdHolder.get(), euWest.endpoints().getLocalEndpointId())) {
                this.leaderGrid.set(this.euWest);
            }
        });
        usEast.events().changedLeaderId().subscribe(leaderIdHolder -> {
            if (leaderIdHolder.isEmpty()) {
                var currentLeader = this.leaderGrid.get();
                if (currentLeader != null && UuidTools.equals(currentLeader.endpoints().getLocalEndpointId(), usEast.endpoints().getLocalEndpointId())) {
                    this.leaderGrid.set(null);
                }
            } else if (UuidTools.equals(leaderIdHolder.get(), usEast.endpoints().getLocalEndpointId())) {
                this.leaderGrid.set(this.usEast);
            }
        });
        asEast.events().changedLeaderId().subscribe(leaderIdHolder -> {
            if (leaderIdHolder.isEmpty()) {
                var currentLeader = this.leaderGrid.get();
                if (currentLeader != null && UuidTools.equals(currentLeader.endpoints().getLocalEndpointId(), asEast.endpoints().getLocalEndpointId())) {
                    this.leaderGrid.set(null);
                }
            } else if (UuidTools.equals(leaderIdHolder.get(), asEast.endpoints().getLocalEndpointId())) {
                this.leaderGrid.set(this.asEast);
            }
        });

        this.router.add(euWest.endpoints().getLocalEndpointId(), euWest.transport());
        this.router.add(usEast.endpoints().getLocalEndpointId(), usEast.transport());
        this.router.add(asEast.endpoints().getLocalEndpointId(), asEast.transport());

        if (!this.autoDiscovery) {
            return;
        }


        if (0 < timeoutInMs) {
            CompletableFuture.allOf(euWestIsReady, usEastIsReady, asEastIsReady).get(timeoutInMs, TimeUnit.MILLISECONDS);
        } else {
            CompletableFuture.allOf(euWestIsReady, usEastIsReady, asEastIsReady).get();
        }
    }

    public void awaitLeader(int timeoutInMs) throws InterruptedException {
        var started = Instant.now().toEpochMilli();
        while (this.leaderGrid.get() == null) {
            var now = Instant.now().toEpochMilli();
            if (0 < timeoutInMs && timeoutInMs < now - started) {
                throw new IllegalStateException("Timeout occurred while waiting for leader");
            }
            Thread.sleep(1000);
        }
    }

    public StorageGrid getEuWestGrid() {
        return this.euWest;
    }

    public StorageGrid getUsEastGrid() {
        return this.usEast;
    }

    public StorageGrid getAsEastGrid() {
        return this.asEast;
    }

    public StorageGrid getLeaderGrid() {
        return this.leaderGrid.get();
    }

    public List<StorageGrid> getFollowerGrids() {
        if (this.leaderGrid.get() == null) {
            return null;
        }
        return this.grids().stream()
                .filter(grid -> grid != this.leaderGrid.get())
                .collect(Collectors.toList());
    }

    public ReplicatedStoragesClusterEnv awaitUntilCommitsSynced() {
        var commitIndex = this.grids().stream().map(g -> g.raft().getCommitIndex()).max(Integer::compare).get();
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return this;
            }
            var minCommitIndex = this.grids().stream().map(g -> g.raft().getCommitIndex()).min(Integer::compare).get();
            logger.info("Awaiting until commit index everywhere becomes {}. Right now the min is {}", commitIndex, minCommitIndex);
            if (minCommitIndex < commitIndex) {
                continue;
            } else {
                break;
            }
        }
        return this;
    }

    public ReplicatedStorage<String, Integer> getEuStorage() {
        return this.euStorage;
    }

    public ReplicatedStorage<String, Integer> getUsStorage() {
        return this.usStorage;
    }

    public ReplicatedStorage<String, Integer> getAsStorage() {
        return this.asStorage;
    }

    public ReplicatedStorage<String, Integer> getLeaderStorage() {
        this.requireCreated();
        var leaderGrid = this.leaderGrid.get();
        if (leaderGrid == null) {
            return null;
        }

        if (UuidTools.equals(leaderGrid.endpoints().getLocalEndpointId(), this.euWest.endpoints().getLocalEndpointId())) {
            return this.euStorage;
        } else if (UuidTools.equals(leaderGrid.endpoints().getLocalEndpointId(), this.usEast.endpoints().getLocalEndpointId())) {
            return this.usStorage;
        } else {
            return this.asStorage;
        }
    }

    public List<ReplicatedStorage<String, Integer>> getFollowerStorages() {
        this.requireCreated();
        var leaderStorage = this.getLeaderStorage();
        if (leaderStorage == null) {
            return Collections.emptyList();
        }
        return this.storages().stream().filter(s -> s != leaderStorage).collect(Collectors.toList());
    }

    public boolean isEuWestJoined() {
        var endpointId = this.euWest.endpoints().getLocalEndpointId();
        return this.router.isDisabled(endpointId) == false;
    }

    public boolean isUsEastJoined() {
        var endpointId = this.usEast.endpoints().getLocalEndpointId();
        return this.router.isDisabled(endpointId) == false;
    }

    public boolean isAsEastJoined() {
        var endpointId = this.asEast.endpoints().getLocalEndpointId();
        return this.router.isDisabled(endpointId) == false;
    }

    public void joinEuWest(int timeoutInMs) throws ExecutionException, InterruptedException, TimeoutException {
        var endpointId = this.euWest.endpoints().getLocalEndpointId();
        if (!this.router.isDisabled(endpointId)) {
            return;
        }

        var joined_1 = new CompletableFuture<UUID>();
        usEast.events().joinedRemoteEndpoints().subscribe(remoteEndpointId -> {
            if (UuidTools.equals(remoteEndpointId, this.euWest.endpoints().getLocalEndpointId())) {
                joined_1.complete(remoteEndpointId);
            }
        });
        var joined_2 = new CompletableFuture<UUID>();
        asEast.events().joinedRemoteEndpoints().subscribe(remoteEndpointId -> {
            if (UuidTools.equals(remoteEndpointId, this.euWest.endpoints().getLocalEndpointId())) {
                joined_2.complete(remoteEndpointId);
            }
        });


        this.usEast.addRemoteEndpointId(this.euWest.endpoints().getLocalEndpointId());
        this.asEast.addRemoteEndpointId(this.euWest.endpoints().getLocalEndpointId());
        this.euWest.addRemoteEndpointId(this.usEast.endpoints().getLocalEndpointId());
        this.euWest.addRemoteEndpointId(this.asEast.endpoints().getLocalEndpointId());

        if (0 < timeoutInMs) {
            CompletableFuture.allOf(joined_1, joined_2).get(timeoutInMs, TimeUnit.MILLISECONDS);
        } else {
            CompletableFuture.allOf(joined_1, joined_2).get();
        }
        this.router.enable(euWest.endpoints().getLocalEndpointId());
    }

    public void joinUsEast(int timeoutInMs) throws ExecutionException, InterruptedException, TimeoutException {
        var endpointId = this.usEast.endpoints().getLocalEndpointId();
        if (!this.router.isDisabled(endpointId)) {
            return;
        }

        var joined_1 = new CompletableFuture<UUID>();
        euWest.events().joinedRemoteEndpoints().subscribe(remoteEndpointId -> {
            if (UuidTools.equals(remoteEndpointId, this.usEast.endpoints().getLocalEndpointId())) {
                joined_1.complete(remoteEndpointId);
            }
        });
        var joined_2 = new CompletableFuture<UUID>();
        asEast.events().joinedRemoteEndpoints().subscribe(remoteEndpointId -> {
            if (UuidTools.equals(remoteEndpointId, this.usEast.endpoints().getLocalEndpointId())) {
                joined_2.complete(remoteEndpointId);
            }
        });

        this.euWest.addRemoteEndpointId(this.usEast.endpoints().getLocalEndpointId());
        this.asEast.addRemoteEndpointId(this.usEast.endpoints().getLocalEndpointId());
        this.usEast.addRemoteEndpointId(this.euWest.endpoints().getLocalEndpointId());
        this.usEast.addRemoteEndpointId(this.asEast.endpoints().getLocalEndpointId());

        if (0 < timeoutInMs) {
            CompletableFuture.allOf(joined_1, joined_2).get(timeoutInMs, TimeUnit.MILLISECONDS);
        } else {
            CompletableFuture.allOf(joined_1, joined_2).get();
        }
        this.router.enable(usEast.endpoints().getLocalEndpointId());
    }

    public void joinAsEast(int timeoutInMs) throws ExecutionException, InterruptedException, TimeoutException {
        var endpointId = this.asEast.endpoints().getLocalEndpointId();
        if (!this.router.isDisabled(endpointId)) {
            return;
        }

        var joined_1 = new CompletableFuture<UUID>();
        euWest.events().joinedRemoteEndpoints().subscribe(remoteEndpointId -> {
            if (UuidTools.equals(remoteEndpointId, this.asEast.endpoints().getLocalEndpointId())) {
                joined_1.complete(remoteEndpointId);
            }
        });
        var joined_2 = new CompletableFuture<UUID>();
        usEast.events().joinedRemoteEndpoints().subscribe(remoteEndpointId -> {
            if (UuidTools.equals(remoteEndpointId, this.asEast.endpoints().getLocalEndpointId())) {
                joined_2.complete(remoteEndpointId);
            }
        });

        this.euWest.addRemoteEndpointId(this.asEast.endpoints().getLocalEndpointId());
        this.usEast.addRemoteEndpointId(this.asEast.endpoints().getLocalEndpointId());
        this.asEast.addRemoteEndpointId(this.euWest.endpoints().getLocalEndpointId());
        this.asEast.addRemoteEndpointId(this.usEast.endpoints().getLocalEndpointId());

        if (0 < timeoutInMs) {
            CompletableFuture.allOf(joined_1, joined_2).get(timeoutInMs, TimeUnit.MILLISECONDS);
        } else {
            CompletableFuture.allOf(joined_1, joined_2).get();
        }
        this.router.enable(asEast.endpoints().getLocalEndpointId());
    }

    public void detachEuWest(int timeoutInMs) throws ExecutionException, InterruptedException, TimeoutException {
        var stopped_1 = new CompletableFuture<UUID>();
        usEast.events().detachedRemoteEndpoints().subscribe(remoteEndpointId -> {
            if (UuidTools.equals(remoteEndpointId, this.euWest.endpoints().getLocalEndpointId())) {
                stopped_1.complete(remoteEndpointId);
            }
        });
        var stopped_2 = new CompletableFuture<UUID>();
        asEast.events().detachedRemoteEndpoints().subscribe(remoteEndpointId -> {
            if (UuidTools.equals(remoteEndpointId, this.euWest.endpoints().getLocalEndpointId())) {
                stopped_2.complete(remoteEndpointId);
            }
        });

        this.router.disable(euWest.endpoints().getLocalEndpointId());
        this.usEast.removeRemoteEndpointId(this.euWest.endpoints().getLocalEndpointId());
        this.asEast.removeRemoteEndpointId(this.euWest.endpoints().getLocalEndpointId());
        this.euWest.removeRemoteEndpointId(this.usEast.endpoints().getLocalEndpointId());
        this.euWest.removeRemoteEndpointId(this.asEast.endpoints().getLocalEndpointId());

        if (0 < timeoutInMs) {
            CompletableFuture.allOf(stopped_1, stopped_2).get(timeoutInMs, TimeUnit.MILLISECONDS);
        } else {
            CompletableFuture.allOf(stopped_1, stopped_2).get();
        }
    }

    public void detachUsEast(int timeoutInMs) throws ExecutionException, InterruptedException, TimeoutException {
        var stopped_1 = new CompletableFuture<UUID>();
        euWest.events().detachedRemoteEndpoints().subscribe(remoteEndpointId -> {
            if (UuidTools.equals(remoteEndpointId, this.usEast.endpoints().getLocalEndpointId())) {
                stopped_1.complete(remoteEndpointId);
            }
        });
        var stopped_2 = new CompletableFuture<UUID>();
        asEast.events().detachedRemoteEndpoints().subscribe(remoteEndpointId -> {
            if (UuidTools.equals(remoteEndpointId, this.usEast.endpoints().getLocalEndpointId())) {
                stopped_2.complete(remoteEndpointId);
            }
        });

        this.router.disable(usEast.endpoints().getLocalEndpointId());
        this.euWest.removeRemoteEndpointId(this.usEast.endpoints().getLocalEndpointId());
        this.asEast.removeRemoteEndpointId(this.usEast.endpoints().getLocalEndpointId());
        this.usEast.removeRemoteEndpointId(this.euWest.endpoints().getLocalEndpointId());
        this.usEast.removeRemoteEndpointId(this.asEast.endpoints().getLocalEndpointId());

        if (0 < timeoutInMs) {
            CompletableFuture.allOf(stopped_1, stopped_2).get(timeoutInMs, TimeUnit.MILLISECONDS);
        } else {
            CompletableFuture.allOf(stopped_1, stopped_2).get();
        }
    }

    public void detachAsEast(int timeoutInMs) throws ExecutionException, InterruptedException, TimeoutException {
        var stopped_1 = new CompletableFuture<UUID>();
        euWest.events().detachedRemoteEndpoints().subscribe(remoteEndpointId -> {
            if (UuidTools.equals(remoteEndpointId, this.asEast.endpoints().getLocalEndpointId())) {
                stopped_1.complete(remoteEndpointId);
            }
        });
        var stopped_2 = new CompletableFuture<UUID>();
        usEast.events().detachedRemoteEndpoints().subscribe(remoteEndpointId -> {
            if (UuidTools.equals(remoteEndpointId, this.asEast.endpoints().getLocalEndpointId())) {
                stopped_2.complete(remoteEndpointId);
            }
        });

        this.router.disable(asEast.endpoints().getLocalEndpointId());
        this.euWest.removeRemoteEndpointId(this.asEast.endpoints().getLocalEndpointId());
        this.usEast.removeRemoteEndpointId(this.asEast.endpoints().getLocalEndpointId());
        this.asEast.removeRemoteEndpointId(this.euWest.endpoints().getLocalEndpointId());
        this.asEast.removeRemoteEndpointId(this.usEast.endpoints().getLocalEndpointId());

        if (0 < timeoutInMs) {
            CompletableFuture.allOf(stopped_1, stopped_2).get(timeoutInMs, TimeUnit.MILLISECONDS);
        } else {
            CompletableFuture.allOf(stopped_1, stopped_2).get();
        }
    }

    public void detachFollowersGrid(int timeoutInMs) throws ExecutionException, InterruptedException, TimeoutException {
        var leaderGrid = this.getLeaderGrid();
        if (leaderGrid == null) {
            throw new IllegalStateException("Cannot detach follower in an environment where leader is not elected");
        }
        if (UuidTools.equals(leaderGrid.endpoints().getLocalEndpointId(), this.euWest.endpoints().getLocalEndpointId())) {
            this.detachUsEast(timeoutInMs);
            this.detachAsEast(timeoutInMs);
        } else if (UuidTools.equals(leaderGrid.endpoints().getLocalEndpointId(), this.usEast.endpoints().getLocalEndpointId())) {
            this.detachEuWest(timeoutInMs);
            this.detachAsEast(timeoutInMs);
        } else {
            this.detachUsEast(timeoutInMs);
            this.detachEuWest(timeoutInMs);
        }
    }

    public void detachLeaderGrid(int timeoutInMs) throws ExecutionException, InterruptedException, TimeoutException {
        var leaderGrid = this.getLeaderGrid();
        if (leaderGrid == null) {
            throw new IllegalStateException("Cannot detach follower in an environment where leader is not elected");
        }
        if (UuidTools.equals(leaderGrid.endpoints().getLocalEndpointId(), this.euWest.endpoints().getLocalEndpointId())) {
            this.detachEuWest(timeoutInMs);
        } else if (UuidTools.equals(leaderGrid.endpoints().getLocalEndpointId(), this.usEast.endpoints().getLocalEndpointId())) {
            this.detachUsEast(timeoutInMs);
        } else {
            this.detachAsEast(timeoutInMs);
        }
    }


    private void requireNotCreated() {
        this.requireNotCreated("Cannot run operation on a created environment");
    }

    private void requireNotCreated(String message) {
        if (this.created) {
            throw new IllegalStateException(message);
        }
    }

    private void requireCreated() {
        this.requireCreated("Cannot run operation on a non-created environment");
    }

    private void requireCreated(String message) {
        if (!this.created) {
            throw new IllegalStateException(message);
        }
    }

    private List<StorageGrid> grids() {
        return List.of(this.euWest, this.usEast, this.asEast);
    }

    private List<ReplicatedStorage<String, Integer>> storages() {
        return List.of(this.euStorage, this.usStorage, this.asStorage);
    }
}
