package io.github.coderdogman.javamindb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class DBFile implements AutoCloseable {
    static final String DATA_FILE_NAME = "minidb.data";
    static final String MERGE_FILE_NAME = "minidb.data.merge";

    private final Path path;
    private final FileChannel channel;
    private final long dataStartOffset;
    private final boolean legacyFormat;
    private long offset;

    private DBFile(Path path, FileChannel channel, long dataStartOffset, boolean legacyFormat) throws IOException {
        this.path = path;
        this.channel = channel;
        this.dataStartOffset = dataStartOffset;
        this.legacyFormat = legacyFormat;
        this.offset = channel.size();
    }

    static DBFile openData(Path directory) throws IOException {
        Files.createDirectories(directory);
        Path path = directory.resolve(DATA_FILE_NAME);
        FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        boolean success = false;
        try {
            long length = channel.size();
            if (length == 0) {
                writeFully(channel, ByteBuffer.wrap(FileFormat.header()), 0);
                channel.force(true);
                DBFile result = new DBFile(path, channel, FileFormat.FILE_HEADER_SIZE, false);
                success = true;
                return result;
            }

            int prefixLength = (int) Math.min(length, FileFormat.MAGIC.length);
            byte[] prefix = new byte[prefixLength];
            readFully(channel, ByteBuffer.wrap(prefix), 0);
            if (FileFormat.isPartialMagicPrefix(prefix)) {
                throw new CorruptDatabaseException(
                        "truncated JavaMinDB file header: expected " + FileFormat.FILE_HEADER_SIZE
                                + " bytes but found " + length);
            }
            if (prefixLength == FileFormat.MAGIC.length && FileFormat.hasMagic(prefix)) {
                if (length < FileFormat.FILE_HEADER_SIZE) {
                    throw new CorruptDatabaseException(
                            "truncated JavaMinDB file header: expected " + FileFormat.FILE_HEADER_SIZE
                                    + " bytes but found " + length);
                }
                byte[] header = new byte[FileFormat.FILE_HEADER_SIZE];
                readFully(channel, ByteBuffer.wrap(header), 0);
                FileFormat.decode(header);
                DBFile result = new DBFile(path, channel, FileFormat.FILE_HEADER_SIZE, false);
                success = true;
                return result;
            }

            // Phase 0 had no file header. It is accepted as legacy input and rewritten to v1
            // after a successful scan.
            DBFile result = new DBFile(path, channel, 0, true);
            success = true;
            return result;
        } finally {
            if (!success) {
                channel.close();
            }
        }
    }

    static DBFile createMerge(Path directory) throws IOException {
        Files.createDirectories(directory);
        Path mergePath = directory.resolve(MERGE_FILE_NAME);
        Files.deleteIfExists(mergePath);
        FileChannel channel = FileChannel.open(mergePath,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        boolean success = false;
        try {
            writeFully(channel, ByteBuffer.wrap(FileFormat.header()), 0);
            DBFile result = new DBFile(mergePath, channel, FileFormat.FILE_HEADER_SIZE, false);
            success = true;
            return result;
        } finally {
            if (!success) {
                channel.close();
            }
        }
    }

    Entry read(long entryOffset) throws IOException {
        long length = channel.size();
        if (entryOffset == length) {
            return null;
        }
        if (entryOffset < dataStartOffset || entryOffset > length) {
            throw new CorruptDatabaseException("invalid entry offset: " + entryOffset);
        }

        long remaining = length - entryOffset;
        if (remaining < Entry.HEADER_SIZE) {
            throw new TruncatedEntryException(entryOffset,
                    "truncated entry header at offset " + entryOffset + ": " + remaining + " bytes remain");
        }

        byte[] header = new byte[Entry.HEADER_SIZE];
        readFully(channel, ByteBuffer.wrap(header), entryOffset);
        Entry entry = Entry.decodeHeader(header, entryOffset);

        long expectedSize = entry.size();
        if (expectedSize > remaining) {
            throw new TruncatedEntryException(entryOffset,
                    "truncated entry at offset " + entryOffset + ": expected " + expectedSize
                            + " bytes but only " + remaining + " remain");
        }

        long cursor = entryOffset + Entry.HEADER_SIZE;
        byte[] key = new byte[entry.keySize()];
        readFully(channel, ByteBuffer.wrap(key), cursor);
        cursor += entry.keySize();

        byte[] value = new byte[entry.valueSize()];
        if (value.length > 0) {
            readFully(channel, ByteBuffer.wrap(value), cursor);
        }
        return entry.withPayload(key, value);
    }

    long append(Entry entry) throws IOException {
        long writeOffset = offset;
        byte[] encoded = entry.encode();
        writeFully(channel, ByteBuffer.wrap(encoded), writeOffset);
        offset += encoded.length;
        return writeOffset;
    }

    void sync() throws IOException {
        channel.force(true);
    }

    void truncate(long newLength) throws IOException {
        long length = channel.size();
        if (newLength < dataStartOffset || newLength > length) {
            throw new IllegalArgumentException("invalid truncate length: " + newLength);
        }
        channel.truncate(newLength);
        offset = newLength;
        sync();
    }

    long size() throws IOException {
        return channel.size();
    }

    long dataStartOffset() {
        return dataStartOffset;
    }

    boolean isLegacyFormat() {
        return legacyFormat;
    }

    Path path() {
        return path;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        long cursor = position;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, cursor);
            if (read < 0) {
                throw new CorruptDatabaseException("unexpected end of file at offset " + cursor);
            }
            if (read == 0) {
                Thread.onSpinWait();
                continue;
            }
            cursor += read;
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        long cursor = position;
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, cursor);
            if (written == 0) {
                Thread.onSpinWait();
                continue;
            }
            cursor += written;
        }
    }

    static final class TruncatedEntryException extends IOException {
        private final long entryOffset;

        TruncatedEntryException(long entryOffset, String message) {
            super(message);
            this.entryOffset = entryOffset;
        }

        long entryOffset() {
            return entryOffset;
        }
    }
}
