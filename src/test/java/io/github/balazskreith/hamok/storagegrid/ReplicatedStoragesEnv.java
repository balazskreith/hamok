package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.UuidTools;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.Function;

public class ReplicatedStoragesEnv {
    public static final String STORAGE_ID = "Test-Storage-" + UUID.randomUUID().toString().substring(0, 8);
    private StorageGrid euWest;
    private StorageGrid usEast;
    private ReplicatedStorage<String, Integer> euStorage;
    private ReplicatedStorage<String, Integer> usStorage;
    private StorageGridRouter router = new StorageGridRouter();
    private BinaryOperator<Integer> mergeOp = (itemsFromStockPile1, itemsFromStockPile2) -> itemsFromStockPile1 + itemsFromStockPile2;
    private volatile boolean created = false;
    private int maxRetentionTimeInMs = 0;
    private int maxCollectedStorageEvents = 1;
    private int maxCollectedStorageTimeInMs = 0;
    private AtomicReference<StorageGrid> leaderGrid = new AtomicReference<>(null);

    public ReplicatedStoragesEnv setMaxRetention(int maxRetentionTimeInMs) {
        this.requireNotCreated();
        this.maxRetentionTimeInMs = maxRetentionTimeInMs;
        return this;
    }

    public ReplicatedStoragesEnv setMaxCollectedStorageEvents(int maxCollectedStorageEvents) {
        this.requireNotCreated();
        this.maxCollectedStorageEvents = maxCollectedStorageEvents;
        return this;
    }

    public ReplicatedStoragesEnv setMaxCollectedStorageTimeInMs(int maxCollectedStorageTimeInMs) {
        this.requireNotCreated();
        this.maxCollectedStorageTimeInMs = maxCollectedStorageTimeInMs;
        return this;
    }

    public ReplicatedStoragesEnv setMergeOp(BinaryOperator<Integer> mergeOp) {
        this.requireNotCreated();
        this.mergeOp = mergeOp;
        return this;
    }

    public void clear() {
        this.euStorage.clear();
        this.usStorage.clear();
    }

    public ReplicatedStoragesEnv create() {
        this.requireNotCreated();
        this.euWest = StorageGrid.builder()
                .withContext("Eu West")
                .withRaftMaxLogRetentionTimeInMs(this.maxRetentionTimeInMs)
                .build();
        this.usEast = StorageGrid.builder()
                .withContext("US east")
                .withRaftMaxLogRetentionTimeInMs(this.maxRetentionTimeInMs)
                .build();

        Function<Integer, byte[]> intEnc = i -> ByteBuffer.allocate(4).putInt(i).array();
        Function<byte[], Integer> intDec = b -> ByteBuffer.wrap(b).getInt();
        Function<String, byte[]> strEnc = s -> s.getBytes();
        Function<byte[], String> strDec = b -> new String(b);

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

        this.created = true;
        return this;
    }

    public void destroy() {
        this.requireCreated();
        this.router.disable();
        this.euWest.close();
        this.usEast.close();
    }

    public void await() throws ExecutionException, InterruptedException, TimeoutException {
        this.requireCreated();
        this.await(0);
    }

    public void await(int timeoutInMs) throws ExecutionException, InterruptedException, TimeoutException {
        this.requireCreated();
        var euWestIsReady = new CompletableFuture<UUID>();
        var usEastIsReady = new CompletableFuture<UUID>();

        euWest.events().joinedRemoteEndpoints().subscribe(euWestIsReady::complete);
        usEast.events().joinedRemoteEndpoints().subscribe(usEastIsReady::complete);
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

        this.router.add(euWest.endpoints().getLocalEndpointId(), euWest.transport());
        this.router.add(usEast.endpoints().getLocalEndpointId(), usEast.transport());

        this.usEast.addRemoteEndpointId(this.euWest.endpoints().getLocalEndpointId());
        this.euWest.addRemoteEndpointId(this.usEast.endpoints().getLocalEndpointId());
    }

    public void awaitLeader(int timeoutInMs) throws InterruptedException, ExecutionException, TimeoutException {
        var started = Instant.now().toEpochMilli();
        while (this.leaderGrid.get() == null || this.euStorage.isStandalone() || this.usStorage.isStandalone()) {
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

    public StorageGrid getLeaderGrid() {
        return this.leaderGrid.get();
    }

    public StorageGrid getFollowerGrid() {
        if (this.leaderGrid.get() == null) {
            return null;
        }
        if (this.leaderGrid.get() == this.euWest) {
            return this.usEast;
        } else {
            return this.euWest;
        }
    }

    public ReplicatedStoragesEnv awaitUntilCommitsSynced() {
        var commitIndex = Math.max(this.euWest.raft().getCommitIndex(), this.usEast.raft().getCommitIndex());
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return this;
            }
            var minCommitIndex = Math.min(this.euWest.raft().getCommitIndex(), this.usEast.raft().getCommitIndex());
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

    public ReplicatedStorage<String, Integer> getLeaderStorage() {
        this.requireCreated();
        var leaderGrid = this.leaderGrid.get();
        if (leaderGrid == null) {
            return null;
        }
        if (UuidTools.equals(leaderGrid.endpoints().getLocalEndpointId(), this.euWest.endpoints().getLocalEndpointId())) {
            return this.euStorage;
        } else {
            return this.usStorage;
        }
    }

    public ReplicatedStorage<String, Integer> getFollowerStorage() {
        this.requireCreated();
        var leaderStorage = this.getLeaderStorage();
        if (leaderStorage == null) {
            return null;
        }
        return leaderStorage == this.euStorage ? this.usStorage : this.euStorage;
    }

    public boolean isEuWestJoined() {
        var endpointId = this.euWest.endpoints().getLocalEndpointId();
        return this.router.isDisabled(endpointId) == false;
    }

    public boolean isUsEastJoined() {
        var endpointId = this.usEast.endpoints().getLocalEndpointId();
        return this.router.isDisabled(endpointId) == false;
    }

    public void joinEuWest(int timeoutInMs) throws ExecutionException, InterruptedException, TimeoutException {
        var endpointId = this.euWest.endpoints().getLocalEndpointId();
        if (!this.router.isDisabled(endpointId)) {
            return;
        }

        var joined = new CompletableFuture<UUID>();
        usEast.events().joinedRemoteEndpoints().subscribe(remoteEndpointId -> {
            if (UuidTools.equals(remoteEndpointId, this.euWest.endpoints().getLocalEndpointId())) {
                joined.complete(remoteEndpointId);
            }
        });

        this.usEast.addRemoteEndpointId(this.euWest.endpoints().getLocalEndpointId());
        this.euWest.addRemoteEndpointId(this.usEast.endpoints().getLocalEndpointId());

        if (0 < timeoutInMs) {
            joined.get(timeoutInMs, TimeUnit.MILLISECONDS);
        } else {
            joined.get();
        }
        this.router.enable(euWest.endpoints().getLocalEndpointId());
    }

    public void joinUsEast(int timeoutInMs) throws ExecutionException, InterruptedException, TimeoutException {
        var endpointId = this.usEast.endpoints().getLocalEndpointId();
        if (!this.router.isDisabled(endpointId)) {
            return;
        }

        var joined = new CompletableFuture<UUID>();
        euWest.events().joinedRemoteEndpoints().subscribe(remoteEndpointId -> {
            if (UuidTools.equals(remoteEndpointId, this.usEast.endpoints().getLocalEndpointId())) {
                joined.complete(remoteEndpointId);
            }
        });

        this.usEast.addRemoteEndpointId(this.euWest.endpoints().getLocalEndpointId());
        this.euWest.addRemoteEndpointId(this.usEast.endpoints().getLocalEndpointId());

        if (0 < timeoutInMs) {
            joined.get(timeoutInMs, TimeUnit.MILLISECONDS);
        } else {
            joined.get();
        }
        this.router.enable(usEast.endpoints().getLocalEndpointId());

    }

    public void detachEuWest(int timeoutInMs) throws ExecutionException, InterruptedException, TimeoutException {
        var stopped = new CompletableFuture<UUID>();
        usEast.events().detachedRemoteEndpoints().subscribe(remoteEndpointId -> {
            if (UuidTools.equals(remoteEndpointId, this.euWest.endpoints().getLocalEndpointId())) {
                stopped.complete(remoteEndpointId);
            }
        });

        this.router.disable(euWest.endpoints().getLocalEndpointId());
        this.usEast.removeRemoteEndpointId(this.euWest.endpoints().getLocalEndpointId());
        this.euWest.removeRemoteEndpointId(this.usEast.endpoints().getLocalEndpointId());

        if (0 < timeoutInMs) {
            stopped.get(timeoutInMs, TimeUnit.MILLISECONDS);
        } else {
            stopped.get();
        }
    }

    public void detachUsEast(int timeoutInMs) throws ExecutionException, InterruptedException, TimeoutException {
        var stopped = new CompletableFuture<UUID>();
        euWest.events().detachedRemoteEndpoints().subscribe(remoteEndpointId -> {
            if (UuidTools.equals(remoteEndpointId, this.usEast.endpoints().getLocalEndpointId())) {
                stopped.complete(remoteEndpointId);
            }
        });

        this.router.disable(usEast.endpoints().getLocalEndpointId());
        this.usEast.removeRemoteEndpointId(this.euWest.endpoints().getLocalEndpointId());
        this.euWest.removeRemoteEndpointId(this.usEast.endpoints().getLocalEndpointId());

        if (0 < timeoutInMs) {
            stopped.get(timeoutInMs, TimeUnit.MILLISECONDS);
        } else {
            stopped.get();
        }
    }

    public void detachFollowerGrid(int timeoutInMs) throws ExecutionException, InterruptedException, TimeoutException {
        var leaderGrid = this.getLeaderGrid();
        if (leaderGrid == null) {
            throw new IllegalStateException("Cannot detach follower in an environment where leader is not elected");
        }
        if (UuidTools.equals(leaderGrid.endpoints().getLocalEndpointId(), this.euWest.endpoints().getLocalEndpointId())) {
            this.detachUsEast(timeoutInMs);
        } else {
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
        } else {
            this.detachUsEast(timeoutInMs);
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
}
