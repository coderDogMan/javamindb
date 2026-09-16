package io.github.coderdogman.javamindb;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A tiny append-only embedded key-value store.
 *
 * <p>This is the Phase 0 baseline for JavaMinDB. The public API is intentionally small while
 * persistence and restart behavior are made testable before the project evolves toward a fuller
 * LSM architecture.</p>
 */
public final class MiniDB implements AutoCloseable {
    private final Path directory;
    private final Map<ByteArrayKey, Long> indexes = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private DBFile dbFile;
    private boolean closed;

    private MiniDB(Path directory, DBFile dbFile) {
        this.directory = directory;
        this.dbFile = dbFile;
    }

    public static MiniDB open(String directory) throws IOException {
        return open(Path.of(directory));
    }

    public static MiniDB open(Path directory) throws IOException {
        Path normalized = directory.toAbsolutePath().normalize();
        Files.createDirectories(normalized);
        MiniDB db = new MiniDB(normalized, DBFile.openData(normalized));
        db.loadIndexesFromFile();
        return db;
    }

    public void put(byte[] key, byte[] value) throws IOException {
        requireKey(key);
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }

        lock.writeLock().lock();
        try {
            ensureOpen();
            Entry entry = new Entry(key, value, Entry.PUT);
            long offset = dbFile.append(entry);
            indexes.put(new ByteArrayKey(key), offset);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public byte[] get(byte[] key) throws IOException {
        requireKey(key);

        lock.readLock().lock();
        try {
            ensureOpen();
            Long offset = indexes.get(new ByteArrayKey(key));
            if (offset == null) {
                return null;
            }
            Entry entry = dbFile.read(offset);
            if (entry == null || entry.mark() == Entry.DELETE) {
                return null;
            }
            return entry.value();
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean delete(byte[] key) throws IOException {
        requireKey(key);

        lock.writeLock().lock();
        try {
            ensureOpen();
            ByteArrayKey lookupKey = new ByteArrayKey(key);
            if (!indexes.containsKey(lookupKey)) {
                return false;
            }
            dbFile.append(new Entry(key, new byte[0], Entry.DELETE));
            indexes.remove(lookupKey);
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Rewrites the current live entries into a fresh data file.
     *
     * <p>Phase 0 keeps this operation explicit. Later phases will replace this with a real LSM
     * compaction design.</p>
     */
    public void merge() throws IOException {
        lock.writeLock().lock();
        try {
            ensureOpen();

            Map<ByteArrayKey, Long> rebuiltIndexes = new HashMap<>();
            Path mergePath;

            try (DBFile mergeFile = DBFile.createMerge(directory)) {
                mergePath = mergeFile.path();
                long offset = 0;
                while (offset < dbFile.size()) {
                    Entry entry = dbFile.read(offset);
                    if (entry == null) {
                        break;
                    }

                    ByteArrayKey key = new ByteArrayKey(entry.key());
                    Long currentOffset = indexes.get(key);
                    if (entry.mark() == Entry.PUT && currentOffset != null && currentOffset == offset) {
                        long newOffset = mergeFile.append(entry);
                        rebuiltIndexes.put(key, newOffset);
                    }
                    offset += entry.size();
                }
            }

            Path dataPath = directory.resolve(DBFile.DATA_FILE_NAME);
            dbFile.close();
            try {
                try {
                    Files.move(mergePath, dataPath,
                            StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException ignored) {
                    Files.move(mergePath, dataPath, StandardCopyOption.REPLACE_EXISTING);
                }
                dbFile = DBFile.openData(directory);
                indexes.clear();
                indexes.putAll(rebuiltIndexes);
            } catch (IOException moveFailure) {
                dbFile = DBFile.openData(directory);
                indexes.clear();
                loadIndexesFromFile();
                throw moveFailure;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long size() {
        lock.readLock().lock();
        try {
            return indexes.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    private void loadIndexesFromFile() throws IOException {
        long offset = 0;
        while (offset < dbFile.size()) {
            Entry entry = dbFile.read(offset);
            if (entry == null) {
                break;
            }

            ByteArrayKey key = new ByteArrayKey(entry.key());
            if (entry.mark() == Entry.DELETE) {
                indexes.remove(key);
            } else {
                indexes.put(key, offset);
            }
            offset += entry.size();
        }
    }

    private static void requireKey(byte[] key) {
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("key must not be null or empty");
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("database is closed");
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (!closed) {
                closed = true;
                dbFile.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static final class ByteArrayKey {
        private final byte[] bytes;
        private final int hash;

        private ByteArrayKey(byte[] bytes) {
            this.bytes = Arrays.copyOf(bytes, bytes.length);
            this.hash = Arrays.hashCode(this.bytes);
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
}
