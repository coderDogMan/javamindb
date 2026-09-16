package io.github.coderdogman.javamindb;

import java.util.Arrays;

/** Unsigned lexicographic byte-array key used by the ordered in-memory structures. */
final class ByteArrayKey implements Comparable<ByteArrayKey> {
    private final byte[] bytes;
    private final int hash;

    ByteArrayKey(byte[] bytes) {
        this.bytes = Arrays.copyOf(bytes, bytes.length);
        this.hash = Arrays.hashCode(this.bytes);
    }

    byte[] bytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    @Override
    public int compareTo(ByteArrayKey other) {
        int length = Math.min(bytes.length, other.bytes.length);
        for (int i = 0; i < length; i++) {
            int left = Byte.toUnsignedInt(bytes[i]);
            int right = Byte.toUnsignedInt(other.bytes[i]);
            if (left != right) {
                return Integer.compare(left, right);
            }
        }
        return Integer.compare(bytes.length, other.bytes.length);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ByteArrayKey key && Arrays.equals(bytes, key.bytes);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
