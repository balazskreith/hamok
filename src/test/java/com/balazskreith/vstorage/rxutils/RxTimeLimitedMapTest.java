package com.balazskreith.vstorage.rxutils;

import com.balazskreith.vstorage.common.KeyValuePair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

class RxTimeLimitedMapTest {

    @Test
    void shouldExpireItems() throws InterruptedException {
        var map = new RxTimeLimitedMap<Integer, String>(100);
        var future = new AtomicInteger();

        map.expiredEntry().map(KeyValuePair::getKey).subscribe(future::set);
        map.put(1, "one");
        Thread.sleep(500);
        map.put(2, "two");

        Assertions.assertEquals(1, future.get());
    }
}