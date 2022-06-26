package io.github.balazskreith.vstorage.storagegrid;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CorrespondedStorageTest {

    @Test
    void shouldCreateEmpty() {
        var correspondedStorage = CorrespondedStorage.createEmpty();

        Assertions.assertNull(correspondedStorage.storage());
        Assertions.assertEquals(0, correspondedStorage.endpoints().size());
    }

}