package io.github.coderdogman.javamindb;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32C;

final class Entry {
    static final int V1_HEADER_SIZE = 10;
    static final int V2_HEADER_SIZE = 22;
    static final short PUT = 1;
    static final short DELETE = 2;

    private final byte[] key;
    private final byte[] value;
    private final int keySize;
    private final int valueSize;
    private final short mark;
    private final long sequence;
    private final int checksum;
    private final int headerSize;
    private final boolean checksummed;

    Entry(byte[] key, byte[] value, short mark, long sequence) {
        this(
                copy(key),
                copy(value),
                key == null ? 0 : key.length,
                value == null ? 0 : value.length,
                mark,
                sequence,
                0,
                V2_HEADER_SIZE,
                true);
    }

    private Entry(
            byte[] key,
            byte[] value,
            int keySize,
            int valueSize,
            short mark,
            long sequence,
            int checksum,
            int headerSize,
            boolean checksummed) {
        this.key = key;
        this.value = value;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.mark = mark;
        this.sequence = sequence;
        this.checksum = checksum;
        this.headerSize = headerSize;
        this.checksummed = checksummed;
    }

    long size() {
        return (long) headerSize + keySize + valueSize;
    }

    byte[] encodeV2() {
        if (key == null) {
            throw new IllegalStateException("entry key is required");
        }
        validateForWrite();
        int crc = calculateChecksum(keySize, valueSize, mark, sequence, key, value);
        ByteBuffer buffer = ByteBuffer.allocate(Math.toIntExact(size()));
        buffer.putInt(keySize);
        buffer.putInt(valueSize);
        buffer.putShort(mark);
        buffer.putLong(sequence);
        buffer.putInt(crc);
        buffer.put(key);
        if (value != null) {
            buffer.put(value);
        }
        return buffer.array();
    }

    static Entry decodeV1Header(byte[] header, long offset) throws CorruptDatabaseException {
        if (header.length != V1_HEADER_SIZE) {
            throw new CorruptDatabaseException("invalid v1 entry header length at offset " + offset);
        }
        ByteBuffer buffer = ByteBuffer.wrap(header);
        int keySize = buffer.getInt();
        int valueSize = buffer.getInt();
        short mark = buffer.getShort();
        validateMetadata(keySize, valueSize, mark, offset);
        return new Entry(null, null, keySize, valueSize, mark, 0L, 0, V1_HEADER_SIZE, false);
    }

    static Entry decodeV2Header(byte[] header, long offset) throws CorruptDatabaseException {
        if (header.length != V2_HEADER_SIZE) {
            throw new CorruptDatabaseException("invalid v2 entry header length at offset " + offset);
        }
        ByteBuffer buffer = ByteBuffer.wrap(header);
        int keySize = buffer.getInt();
        int valueSize = buffer.getInt();
        short mark = buffer.getShort();
        long sequence = buffer.getLong();
        int checksum = buffer.getInt();
        validateMetadata(keySize, valueSize, mark, offset);
        if (sequence <= 0) {
            throw new CorruptDatabaseException("invalid sequence " + sequence + " at offset " + offset);
        }
        return new Entry(null, null, keySize, valueSize, mark, sequence, checksum, V2_HEADER_SIZE, true);
    }

    Entry withPayload(byte[] key, byte[] value, long offset) throws CorruptDatabaseException {
        if (key.length != keySize || value.length != valueSize) {
            throw new IllegalArgumentException("payload size does not match entry header");
        }
        if (checksummed) {
            int actual = calculateChecksum(keySize, valueSize, mark, sequence, key, value);
            if (actual != checksum) {
                throw new CorruptDatabaseException(
                        "CRC32C mismatch for entry at offset " + offset
                                + ": expected " + Integer.toUnsignedString(checksum)
                                + " but calculated " + Integer.toUnsignedString(actual));
            }
        }
        return new Entry(
                copy(key),
                copy(value),
                keySize,
                valueSize,
                mark,
                sequence,
                checksum,
                headerSize,
                checksummed);
    }

    Entry withSequence(long newSequence) {
        if (key == null) {
            throw new IllegalStateException("entry payload is required");
        }
        return new Entry(key, value, mark, newSequence);
    }

    byte[] key() {
        return copy(key);
    }

    byte[] value() {
        return copy(value);
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

    long sequence() {
        return sequence;
    }

    boolean checksummed() {
        return checksummed;
    }

    private void validateForWrite() {
        try {
            validateMetadata(keySize, valueSize, mark, -1);
        } catch (CorruptDatabaseException impossible) {
            throw new IllegalArgumentException(impossible.getMessage(), impossible);
        }
        if (sequence <= 0) {
            throw new IllegalArgumentException("sequence must be positive");
        }
    }

    private static void validateMetadata(int keySize, int valueSize, short mark, long offset)
            throws CorruptDatabaseException {
        String suffix = offset >= 0 ? " at offset " + offset : "";
        if (keySize <= 0 || keySize > MiniDB.MAX_KEY_SIZE) {
            throw new CorruptDatabaseException("invalid key size " + keySize + suffix);
        }
        if (valueSize < 0 || valueSize > MiniDB.MAX_VALUE_SIZE) {
            throw new CorruptDatabaseException("invalid value size " + valueSize + suffix);
        }
        if (mark != PUT && mark != DELETE) {
            throw new CorruptDatabaseException("invalid entry operation " + mark + suffix);
        }
        if (mark == DELETE && valueSize != 0) {
            throw new CorruptDatabaseException("delete entry has a value" + suffix);
        }
    }

    private static int calculateChecksum(
            int keySize,
            int valueSize,
            short mark,
            long sequence,
            byte[] key,
            byte[] value) {
        CRC32C crc = new CRC32C();
        ByteBuffer metadata = ByteBuffer.allocate(18);
        metadata.putInt(keySize);
        metadata.putInt(valueSize);
        metadata.putShort(mark);
        metadata.putLong(sequence);
        crc.update(metadata.array(), 0, metadata.array().length);
        crc.update(key, 0, key.length);
        if (value != null && value.length > 0) {
            crc.update(value, 0, value.length);
        }
        return (int) crc.getValue();
    }

    private static byte[] copy(byte[] bytes) {
        return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
    }
}
