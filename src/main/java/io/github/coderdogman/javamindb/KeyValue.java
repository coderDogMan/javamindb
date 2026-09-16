package io.github.coderdogman.javamindb;

import java.util.Arrays;

/** Immutable key/value pair returned by ordered iteration APIs. */
public final class KeyValue {
    private final byte[] key;
    private final byte[] value;

    KeyValue(byte[] key, byte[] value) {
        this.key = Arrays.copyOf(key, key.length);
        this.value = Arrays.copyOf(value, value.length);
    }

    public byte[] key() {
        return Arrays.copyOf(key, key.length);
    }

    public byte[] value() {
        return Arrays.copyOf(value, value.length);
    }
}
