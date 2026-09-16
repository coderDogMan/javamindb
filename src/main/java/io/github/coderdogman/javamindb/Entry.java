package io.github.coderdogman.javamindb;

import java.nio.ByteBuffer;
import java.util.Arrays;

final class Entry {
    static final int HEADER_SIZE = 10;
    static final short PUT = 1;
    static final short DELETE = 2;
    private static final int MAX_COMPONENT_SIZE = 64 * 1024 * 1024;

    private final byte[] key;
    private final byte[] value;
    private final int keySize;
    private final int valueSize;
    private final short mark;

    Entry(byte[] key, byte[] value, short mark) {
        this(key, value, key == null ? 0 : key.length, value == null ? 0 : value.length, mark);
    }

    private Entry(byte[] key, byte[] value, int keySize, int valueSize, short mark) {
        this.key = key == null ? null : Arrays.copyOf(key, key.length);
        this.value = value == null ? null : Arrays.copyOf(value, value.length);
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.mark = mark;
    }

    long size() {
        return (long) HEADER_SIZE + keySize + valueSize;
    }

    byte[] encode() {
        if (key == null) {
            throw new IllegalStateException("entry key is required");
        }
        ByteBuffer buffer = ByteBuffer.allocate(Math.toIntExact(size()));
        buffer.putInt(keySize);
        buffer.putInt(valueSize);
        buffer.putShort(mark);
        buffer.put(key);
        if (value != null) {
            buffer.put(value);
        }
        return buffer.array();
    }

    static Entry decodeHeader(byte[] header) throws java.io.IOException {
        if (header.length != HEADER_SIZE) {
            throw new java.io.IOException("invalid entry header length: " + header.length);
        }
        ByteBuffer buffer = ByteBuffer.wrap(header);
        int keySize = buffer.getInt();
        int valueSize = buffer.getInt();
        short mark = buffer.getShort();

        if (keySize <= 0 || keySize > MAX_COMPONENT_SIZE) {
            throw new java.io.IOException("invalid key size: " + keySize);
        }
        if (valueSize < 0 || valueSize > MAX_COMPONENT_SIZE) {
            throw new java.io.IOException("invalid value size: " + valueSize);
        }
        if (mark != PUT && mark != DELETE) {
            throw new java.io.IOException("invalid entry mark: " + mark);
        }

        return new Entry(null, null, keySize, valueSize, mark);
    }

    Entry withPayload(byte[] key, byte[] value) {
        if (key.length != keySize || value.length != valueSize) {
            throw new IllegalArgumentException("payload size does not match entry header");
        }
        return new Entry(key, value, keySize, valueSize, mark);
    }

    byte[] key() {
        return Arrays.copyOf(key, key.length);
    }

    byte[] value() {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }

    int keySize() {
        return keySize;
    }

    int valueSize() {
        return valueSize;
    }

    short mark() {
        return mark;
    }
}
