package io.github.balazskreith.hamok.storagegrid.messages;

public enum MessageType {

    /**
     * Hello notifications are sent by every endpoint in order to inform every other endpoint
     * about the existance.
     */
    HELLO_NOTIFICATION,
    /**
     * Endpoint states are sent by the leader of the grid.
     * Endpoint state contains information about active and inactive endpoints, so every endpoint can keep up to date
     * about the remote endpoints
     */
    ENDPOINT_STATES_NOTIFICATION,

    /**
     * Storage sync is requested by a follower endpoint because it cannot follow the leader. In this case
     * leader should dump every entry from storages synchronized by the endpoint (e.g.: replicated storage) and send it to the follower
     */
    STORAGE_SYNC_REQUEST,
    /**
     * A sync response sent by the leader to the follower contains all entries plus the commit index of the raft
     * the state is corresponded to, so the follower can refresh its raft and commit index.
     */
    STORAGE_SYNC_RESPONSE,

    RAFT_VOTE_REQUEST,
    RAFT_VOTE_RESPONSE,
    RAFT_APPEND_ENTRIES_REQUEST,
    RAFT_APPEND_ENTRIES_RESPONSE,

    /**
     * Submitting entries to the leader
     */
    SUBMIT_REQUEST,
    /**
     * Leader response about the submitting
     */
    SUBMIT_RESPONSE,

    /**
     * Request entries from remote endpoint(s).
     */
    GET_ENTRIES_REQUEST,
    /**
     * Responded entries
     */
    GET_ENTRIES_RESPONSE,

    GET_SIZE_REQUEST,

    GET_SIZE_RESPONSE,

    GET_KEYS_REQUEST,
    GET_KEYS_RESPONSE,

    /**
     * Request to delete entries on a remote endpoint
     */
    DELETE_ENTRIES_REQUEST,
    /**
     * Response to a delete request
     */
    DELETE_ENTRIES_RESPONSE,

    /**
     * Insert item(s) only if they don't exist. if they
     * exist then it returns with the value associated
     *
     * NOTE: Only the storage entries replicated by a
     * distributed and coordinated way like by a Raft algorithm
     * can guarantee that insert is atomic
     *
     */
    INSERT_ENTRIES_REQUEST,
    /**
     * Response to insert requests
     */
    INSERT_ENTRIES_RESPONSE,

    /**
     * Request an update from a remote storage
     */
    UPDATE_ENTRIES_REQUEST,
    /**
     * Response to an update request
     */
    UPDATE_ENTRIES_RESPONSE,

    /**
     * Notification about an insert operation.
     */
    INSERT_ENTRIES_NOTIFICATION,
    /**
     * Notification about the update
     */
    UPDATE_ENTRIES_NOTIFICATION,
    /**
     * Notification about deleting
     */
    DELETE_ENTRIES_NOTIFICATION,
    /**
     * Evict entries
     */
    EVICT_ENTRIES_NOTIFICATION,

    /**
     * Clear entries notification
     */
    CLEAR_ENTRIES_NOTIFICATION,

    ;



    public static MessageType valueOfOrNull(String name) {
        try {
            return MessageType.valueOf(name);
        } catch (Exception ex) {
            return null;
        }
    }
}
