package io.github.coderdogman.javamindb;

import java.nio.ByteBuffer;
import java.util.Arrays;

final class FileFormat {
    static final byte[] MAGIC = new byte[]{'J', 'M', 'I', 'N', 'I', 'D', 'B', 0};
    static final int LEGACY_VERSION = 0;
    static final int V1 = 1;
    static final int VERSION = 2;
    static final int FILE_HEADER_SIZE = 16;

    private FileFormat() {
    }

    static byte[] header() {
        return header(VERSION);
    }

    static byte[] header(int version) {
        ByteBuffer buffer = ByteBuffer.allocate(FILE_HEADER_SIZE);
        buffer.put(MAGIC);
        buffer.putInt(version);
        buffer.putInt(FILE_HEADER_SIZE);
        return buffer.array();
    }

    static Header decode(byte[] bytes) throws CorruptDatabaseException, UnsupportedFormatException {
        if (bytes.length != FILE_HEADER_SIZE) {
            throw new CorruptDatabaseException("invalid file header length: " + bytes.length);
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte[] magic = new byte[MAGIC.length];
        buffer.get(magic);
        if (!Arrays.equals(magic, MAGIC)) {
            throw new CorruptDatabaseException("invalid JavaMinDB file magic");
        }

        int version = buffer.getInt();
        int headerSize = buffer.getInt();
        if (headerSize != FILE_HEADER_SIZE) {
            throw new CorruptDatabaseException("invalid file header size: " + headerSize);
        }
        if (version != V1 && version != VERSION) {
            throw new UnsupportedFormatException(
                    "unsupported JavaMinDB format version " + version
                            + "; supported versions are " + V1 + " and " + VERSION);
        }
        return new Header(version, headerSize);
    }

    static boolean hasMagic(byte[] prefix) {
        return Arrays.equals(prefix, MAGIC);
    }

    static boolean isPartialMagicPrefix(byte[] prefix) {
        if (prefix.length >= MAGIC.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (prefix[i] != MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    record Header(int version, int headerSize) {
    }
}
