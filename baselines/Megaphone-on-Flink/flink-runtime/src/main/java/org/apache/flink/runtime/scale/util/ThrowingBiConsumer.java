package org.apache.flink.runtime.scale.util;
import org.apache.flink.annotation.Public;

/**
 * This interface is basically Java's {@link java.util.function.Consumer} interface enhanced with
 * the ability to throw an exception.
 *
 * @param <T> type of the consumed elements.
 * @param <E> type of the exception thrown.
 */
@Public
@FunctionalInterface
public interface ThrowingBiConsumer<T, U, E extends Exception> {

    /**
     * Consume the given value.
     *
     * @param value the value to consume.
     * @throws E if the consumption fails.
     */
    void accept(T value, U value2) throws E;
}
