package io.github.balazskreith.hamok.common;

import java.util.function.Consumer;
import java.util.function.Supplier;

public interface Depot<T> extends Consumer<T>, Supplier<T> {
}
