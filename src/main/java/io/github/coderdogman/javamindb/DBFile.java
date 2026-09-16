package io.github.coderdogman.javamindb;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

final class DBFile implements AutoCloseable {
    static final String DATA_FILE_NAME = "minidb.data";
    static final String MERGE_FILE_NAME = "minidb.data.merge";

    private final Path path;
    private final RandomAccessFile file;
    private long offset;

    private DBFile(Path path, RandomAccessFile file, long offset) {
        this.path = path;
        this.file = file;
        this.offset = offset;
    }

    static DBFile openData(Path directory) throws IOException {
        Files.createDirectories(directory);
        return open(directory.resolve(DATA_FILE_NAME));
    }

    static DBFile createMerge(Path directory) throws IOException {
        Files.createDirectories(directory);
        Path mergePath = directory.resolve(MERGE_FILE_NAME);
        Files.deleteIfExists(mergePath);
        return open(mergePath);
    }

    private static DBFile open(Path path) throws IOException {
        RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw");
        return new DBFile(path, file, file.length());
    }

    Entry read(long entryOffset) throws IOException {
        if (entryOffset < 0 || entryOffset >= file.length()) {
            return null;
        }

        byte[] header = new byte[Entry.HEADER_SIZE];
        file.seek(entryOffset);
        try {
            file.readFully(header);
        } catch (EOFException error) {
            return null;
        }

        Entry entry = Entry.decodeHeader(header);
        long cursor = entryOffset + Entry.HEADER_SIZE;

        byte[] key = new byte[entry.keySize()];
        file.seek(cursor);
        try {
            file.readFully(key);
        } catch (EOFException error) {
            return null;
        }
        cursor += entry.keySize();

        byte[] value = new byte[entry.valueSize()];
        if (value.length > 0) {
            file.seek(cursor);
            try {
                file.readFully(value);
            } catch (EOFException error) {
                return null;
            }
        }

        return entry.withPayload(key, value);
    }

    long append(Entry entry) throws IOException {
        long writeOffset = offset;
        byte[] encoded = entry.encode();
        file.seek(writeOffset);
        file.write(encoded);
        offset += encoded.length;
        return writeOffset;
    }

    long size() {
        return offset;
    }

    Path path() {
        return path;
    }

    @Override
    public void close() throws IOException {
        file.close();
    }
}
