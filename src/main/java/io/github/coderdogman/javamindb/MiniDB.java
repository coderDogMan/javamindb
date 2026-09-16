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
 * <p>JavaMinDB v0.1 keeps a deliberately small API. Keys and values are byte arrays, writes are
 * append-only, and the live key index is rebuilt when the database opens. The database directory
 * is exclusively locked while an instance is open.</p>
 */
public final class MiniDB implements AutoCloseable {
    /** Maximum supported key size in bytes. */
    public static final int MAX_KEY_SIZE = 64 * 1024 * 1024;
    /** Maximum supported value size in bytes. */
    public static final int MAX_VALUE_SIZE = 64 * 1024 * 1024;

    private final Path directory;
    private final DirectoryLock directoryLock;
    private final Map<ByteArrayKey, Long> indexes = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private DBFile dbFile;
    private boolean closed;

    private MiniDB(Path directory, DirectoryLock directoryLock, DBFile dbFile) {
        this.directory = directory;
        this.directoryLock = directoryLock;
        this.dbFile = dbFile;
    }

    /** Opens or creates a database at {@code directory}. */
    public static MiniDB open(String directory) throws IOException {
        if (directory == null) {
            throw new IllegalArgumentException("directory must not be null");
        }
        return open(Path.of(directory));
    }

    /**
     * Opens or creates a database at {@code directory}.
     *
     * <p>Only one JavaMinDB instance may own a directory at a time. A Phase 0 headerless data file
     * is migrated to the v1 file format after it is scanned successfully. A single incomplete tail
     * record is repaired by truncating back to the last complete record; structural corruption is
     * rejected.</p>
     */
    public static MiniDB open(Path directory) throws IOException {
        if (directory == null) {
            throw new IllegalArgumentException("directory must not be null");
        }

        Path normalized = directory.toAbsolutePath().normalize();
        Files.createDirectories(normalized);
        DirectoryLock directoryLock = DirectoryLock.acquire(normalized);
        DBFile dbFile = null;
        MiniDB db = null;
        try {
            dbFile = DBFile.openData(normalized);
            db = new MiniDB(normalized, directoryLock, dbFile);
            db.loadIndexesFromFile();
            if (dbFile.isLegacyFormat()) {
                db.rewriteCurrentStateToV1();
            }
            return db;
        } catch (IOException | RuntimeException error) {
            if (db != null) {
                db.closeAfterOpenFailure(error);
            } else {
                if (dbFile != null) {
                    try {
                        dbFile.close();
                    } catch (IOException closeError) {
                        error.addSuppressed(closeError);
                    }
                }
                try {
                    directoryLock.close();
                } catch (IOException closeError) {
                    error.addSuppressed(closeError);
                }
            }
            throw error;
        }
    }

    /**
     * Stores {@code value} for {@code key}. Existing values are replaced logically by appending a
     * new record. Empty values are allowed; null values are not.
     */
    public void put(byte[] key, byte[] value) throws IOException {
        requireKey(key);
        requireValue(value);

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

    /** Returns a copy of the stored value, or {@code null} when the key does not exist. */
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

    /**
     * Appends a tombstone for an existing key.
     *
     * @return true if the key existed and was deleted; false when it was already absent
     */
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

    /** Returns the number of currently live keys. */
    public long size() {
        lock.readLock().lock();
        try {
            ensureOpen();
            return indexes.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Requests an fsync of the current append-only data file.
     *
     * <p>This is an explicit persistence boundary for v0.1, not a transactional or WAL guarantee.
     * Phase 2 will define stronger crash-durability semantics.</p>
     */
    public void sync() throws IOException {
        lock.readLock().lock();
        try {
            ensureOpen();
            dbFile.sync();
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Rewrites only live entries into a fresh v1 data file. */
    public void merge() throws IOException {
        lock.writeLock().lock();
        try {
            ensureOpen();
            rewriteCurrentStateToV1();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void rewriteCurrentStateToV1() throws IOException {
        Map<ByteArrayKey, Long> rebuiltIndexes = new HashMap<>();
        Path mergePath;

        try (DBFile mergeFile = DBFile.createMerge(directory)) {
            mergePath = mergeFile.path();
            long offset = dbFile.dataStartOffset();
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
            mergeFile.sync();
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
            try {
                dbFile = DBFile.openData(directory);
                indexes.clear();
                loadIndexesFromFile();
            } catch (IOException recoveryFailure) {
                moveFailure.addSuppressed(recoveryFailure);
            }
            throw moveFailure;
        }
    }

    private void loadIndexesFromFile() throws IOException {
        indexes.clear();
        long offset = dbFile.dataStartOffset();
        while (offset < dbFile.size()) {
            Entry entry;
            try {
                entry = dbFile.read(offset);
            } catch (DBFile.TruncatedEntryException truncated) {
                // Only a physically incomplete final record is recoverable in v0.1. The valid
                // prefix is retained; malformed metadata still fails closed as corruption.
                dbFile.truncate(truncated.entryOffset());
                break;
            }
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
        if (key.length > MAX_KEY_SIZE) {
            throw new IllegalArgumentException("key exceeds max size of " + MAX_KEY_SIZE + " bytes");
        }
    }

    private static void requireValue(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }
        if (value.length > MAX_VALUE_SIZE) {
            throw new IllegalArgumentException("value exceeds max size of " + MAX_VALUE_SIZE + " bytes");
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("database is closed");
        }
    }

    private void closeAfterOpenFailure(Throwable primary) {
        try {
            dbFile.close();
        } catch (IOException closeError) {
            primary.addSuppressed(closeError);
        }
        try {
            directoryLock.close();
        } catch (IOException closeError) {
            primary.addSuppressed(closeError);
        }
        closed = true;
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            IOException failure = null;
            try {
                dbFile.close();
            } catch (IOException error) {
                failure = error;
            }
            try {
                directoryLock.close();
            } catch (IOException error) {
                if (failure == null) {
                    failure = error;
                } else {
                    failure.addSuppressed(error);
                }
            }
            if (failure != null) {
                throw failure;
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
