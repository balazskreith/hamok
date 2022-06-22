package com.balazskreith.vstorage.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class MapUtilsTest {

    @Test
    void shouldMergeDisjointedMaps_1() {
        MapUtils.mergeDisjointMaps();

        // no exception
    }

    @Test
    void shouldMergeDisjointedMaps_2() {
        var map1 = Map.of(1, "one");
        var map2 = Map.of(2, "two");
        var map3 = Map.of(3, "three");
        var mergedMap = MapUtils.mergeDisjointMaps(map1, map2, map3);

        Assertions.assertEquals("one", mergedMap.get(1));
        Assertions.assertEquals("two", mergedMap.get(2));
        Assertions.assertEquals("three", mergedMap.get(3));
    }

    @Test
    void shouldThrowExceptionOnDisjointedMaps_1() {
        var map1 = Map.of(1, "one");
        var map2 = Map.of(1, "two");

        Assertions.assertThrows(Exception.class, () -> MapUtils.mergeDisjointMaps(map1, map2));
    }

    @Test
    void shouldPutMapsTogether_1() {
        var map = MapUtils.putAll(Map.of(1, 1), Map.of(1, 2));

        Assertions.assertEquals(2, map.get(1));
    }

    @Test
    void shouldCreateMapAssignerDepot_1() {
        var depot = MapUtils.makeMapAssignerDepot();
        depot.accept(Map.of(1, "one"));
        depot.accept(Map.of(1, "two"));

        var map = depot.get();
        Assertions.assertEquals("two", map.get(1));
    }

    @Test
    void shouldCreateMapMergerDepot_1() {
        var depot = MapUtils.<Integer, Integer>makeMergedMapDepot((v1, v2) -> v1 + v2);
        depot.accept(Map.of(1, 1));
        depot.accept(Map.of(1, 2));

        var map = depot.get();
        Assertions.assertEquals(3, map.get(1));
    }
}