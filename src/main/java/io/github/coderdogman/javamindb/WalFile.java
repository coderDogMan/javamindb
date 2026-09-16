package io.github.coderdogman.javamindb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

final class WalFile implements AutoCloseable {
    static final String WAL_FILE_NAME = "minidb.wal";
    private static final byte[] MAGIC = new byte[]{'J', 'M', 'W', 'A', 'L', 'D', 'B', 0};
    private static final int VERSION = 1;
    static final int HEADER_SIZE = 16;

    private final Path path;
    private final FileChannel channel;
    private long offset;

    private WalFile(Path path, FileChannel channel) throws IOException {
        this.path = path;
        this.channel = channel;
        this.offset = channel.size();
    }

    static WalFile open(Path directory) throws IOException {
        Files.createDirectories(directory);
        Path path = directory.resolve(WAL_FILE_NAME);
        FileChannel channel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        boolean success = false;
        try {
            if (channel.size() == 0) {
                DBFile.writeFully(channel, ByteBuffer.wrap(header()), 0);
                channel.force(true);
            } else {
                validateHeader(channel);
            }
            WalFile result = new WalFile(path, channel);
            success = true;
            return result;
        } finally {
            if (!success) {
                channel.close();
            }
        }
    }

    long append(Entry entry) throws IOException {
        long writeOffset = offset;
        byte[] encoded = entry.encodeV2();
        DBFile.writeFully(channel, ByteBuffer.wrap(encoded), writeOffset);
        offset += encoded.length;
        return writeOffset;
    }

    Entry read(long entryOffset) throws IOException {
        long length = channel.size();
        if (entryOffset == length) {
            return null;
        }
        if (entryOffset < HEADER_SIZE || entryOffset > length) {
            throw new CorruptDatabaseException("invalid WAL entry offset: " + entryOffset);
        }

        long remaining = length - entryOffset;
        if (remaining < Entry.V2_HEADER_SIZE) {
            throw new TruncatedWalEntryException(entryOffset,
                    "truncated WAL entry header at offset " + entryOffset);
        }

        byte[] header = new byte[Entry.V2_HEADER_SIZE];
        DBFile.readFully(channel, ByteBuffer.wrap(header), entryOffset);
        Entry entry = Entry.decodeV2Header(header, entryOffset);

        if (entry.size() > remaining) {
            throw new TruncatedWalEntryException(
                    entryOffset,
                    "truncated WAL entry at offset " + entryOffset);
        }

        long cursor = entryOffset + Entry.V2_HEADER_SIZE;
        byte[] key = new byte[entry.keySize()];
        DBFile.readFully(channel, ByteBuffer.wrap(key), cursor);
        cursor += entry.keySize();

        byte[] value = new byte[entry.valueSize()];
        if (value.length > 0) {
            DBFile.readFully(channel, ByteBuffer.wrap(value), cursor);
        }
        return entry.withPayload(key, value, entryOffset);
    }

    void sync() throws IOException {
        channel.force(true);
    }

    void reset() throws IOException {
        channel.truncate(HEADER_SIZE);
        offset = HEADER_SIZE;
        channel.force(true);
    }

    void truncate(long newLength) throws IOException {
        if (newLength < HEADER_SIZE || newLength > channel.size()) {
            throw new IllegalArgumentException("invalid WAL truncate length: " + newLength);
        }
        channel.truncate(newLength);
        offset = newLength;
        channel.force(true);
    }

    long size() throws IOException {
        return channel.size();
    }

    long dataStartOffset() {
        return HEADER_SIZE;
    }

    Path path() {
        return path;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private static byte[] header() {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        buffer.put(MAGIC);
        buffer.putInt(VERSION);
        buffer.putInt(HEADER_SIZE);
        return buffer.array();
    }

    private static void validateHeader(FileChannel channel) throws IOException {
        long length = channel.size();
        if (length < HEADER_SIZE) {
            throw new CorruptDatabaseException("truncated JavaMinDB WAL header");
        }
        byte[] header = new byte[HEADER_SIZE];
        DBFile.readFully(channel, ByteBuffer.wrap(header), 0);
        ByteBuffer buffer = ByteBuffer.wrap(header);
        byte[] magic = new byte[MAGIC.length];
        buffer.get(magic);
        if (!Arrays.equals(magic, MAGIC)) {
            throw new CorruptDatabaseException("invalid JavaMinDB WAL magic");
        }
        int version = buffer.getInt();
        int headerSize = buffer.getInt();
        if (version != VERSION) {
            throw new UnsupportedFormatException("unsupported JavaMinDB WAL version " + version);
        }
        if (headerSize != HEADER_SIZE) {
            throw new CorruptDatabaseException("invalid JavaMinDB WAL header size " + headerSize);
        }
    }

    static final class TruncatedWalEntryException extends IOException {
        private final long entryOffset;

        TruncatedWalEntryException(long entryOffset, String message) {
            super(message);
            this.entryOffset = entryOffset;
        }

        long entryOffset() {
            return entryOffset;
        }
    }
}
