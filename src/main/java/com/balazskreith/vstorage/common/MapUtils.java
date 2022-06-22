package com.balazskreith.vstorage.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class MapUtils {

    /**
     * Merge maps into one. Cannot merge duplicated keys and throws an exception
     * @param maps
     * @param <K>
     * @param <V>
     * @return
     */
    @SafeVarargs
    public static<K, V> Map<K, V> mergeDisjointMaps(Map<K, V>... maps) {
        if (maps.length < 1) {
            return Collections.emptyMap();
        } else if (maps.length == 1) {
            return maps[0];
        }
        var stream = Stream.concat(maps[0].entrySet().stream(), maps[1].entrySet().stream());
        for (var i = 2; i < maps.length; ++i) {
            stream = Stream.concat(stream, maps[i].entrySet().stream());
        }
        return stream.collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue
        ));
    }

    public static<K, V> Map<K, V> putAll(Map<K, V>... maps) {
        if (maps.length < 1) {
            return Collections.emptyMap();
        } else if (maps.length == 1) {
            return maps[0];
        }
        if (maps == null) return Collections.emptyMap();
        var depot = MapUtils.<K, V>makeMapAssignerDepot();
        Stream.of(maps).forEach(depot::accept);
        return depot.get();
    }

    public static<K, V> Depot<Map<K, V>> makeMapAssignerDepot() {
        var result = new HashMap<K, V>();
        return new Depot<Map<K, V>>() {
            @Override
            public void accept(Map<K, V> map) {
                result.putAll(map);
            }

            @Override
            public Map<K, V> get() {
                return result;
            }
        };
    }

    public static<K, V> Depot<Map<K, V>> makeMergedMapDepot(BinaryOperator<V> operator) {
        var depot = new LinkedList<Map<K, V>>();
        return new Depot<Map<K, V>>() {
            @Override
            public void accept(Map<K, V> map) {
                depot.add(map);
            }

            @Override
            public Map<K, V> get() {
                if (depot.size() < 1) return Collections.emptyMap();
                else if (depot.size() == 1) return depot.pop();
                var stream = Stream.concat(depot.pop().entrySet().stream(), depot.pop().entrySet().stream());
                while (!depot.isEmpty()) {
                    stream = Stream.concat(stream, depot.pop().entrySet().stream());
                }
                return stream.collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        operator
                ));
            }
        };
    }
}
