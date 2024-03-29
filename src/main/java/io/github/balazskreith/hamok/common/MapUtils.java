package io.github.balazskreith.hamok.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class MapUtils {


    public static <V, K> Map<V, K> invertMap(Map<K, V> map) {
        Map<V, K> result = map.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getValue,
                        Map.Entry::getKey,
                        (old, new_) -> new_
                        )
                );
        return result;
    }

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

    public static<K, V> Map<K, V> combineAll(Map<K, V>... maps) {
        if (maps == null) return Collections.emptyMap();
        if (maps.length < 1) {
            return Collections.emptyMap();
        } else if (maps.length == 1) {
            return maps[0];
        }
        var result = new HashMap<K, V>();
        Stream.of(maps).forEach(result::putAll);
        return Collections.unmodifiableMap(result);
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
                return Collections.unmodifiableMap(result);
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
