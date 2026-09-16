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
 * A tiny append-only embedded key-value store with a write-ahead log.
 *
 * <p>JavaMinDB v0.2 writes each mutation to a CRC32C-protected WAL and forces that WAL before the
 * mutation is appended to the data file. On open, committed WAL records missing from the data file
 * are replayed deterministically.</p>
 */
public final class MiniDB implements AutoCloseable {
    /** Maximum supported key size in bytes. */
    public static final int MAX_KEY_SIZE = 64 * 1024 * 1024;
    /** Maximum supported value size in bytes. */
    public static final int MAX_VALUE_SIZE = 64 * 1024 * 1024;

    private final Path directory;
    private final DirectoryLock directoryLock;
    private final FaultInjector faultInjector;
    private final Map<ByteArrayKey, Long> indexes = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private DBFile dbFile;
    private WalFile walFile;
    private long maxSequence;
    private long nextSequence = 1;
    private boolean recoveryRequired;
    private boolean closed;

    private MiniDB(
            Path directory,
            DirectoryLock directoryLock,
            DBFile dbFile,
            FaultInjector faultInjector) {
        this.directory = directory;
        this.directoryLock = directoryLock;
        this.dbFile = dbFile;
        this.faultInjector = faultInjector;
    }

    /** Opens or creates a database at {@code directory}. */
    public static MiniDB open(String directory) throws IOException {
        if (directory == null) {
            throw new IllegalArgumentException("directory must not be null");
        }
        return open(Path.of(directory));
    }

    /** Opens or creates a database at {@code directory}. */
    public static MiniDB open(Path directory) throws IOException {
        return open(directory, FaultInjector.NONE);
    }

    static MiniDB open(Path directory, FaultInjector faultInjector) throws IOException {
        if (directory == null) {
            throw new IllegalArgumentException("directory must not be null");
        }
        if (faultInjector == null) {
            throw new IllegalArgumentException("faultInjector must not be null");
        }

        Path normalized = directory.toAbsolutePath().normalize();
        Files.createDirectories(normalized);
        DirectoryLock directoryLock = DirectoryLock.acquire(normalized);
        DBFile dbFile = null;
        MiniDB db = null;
        try {
            dbFile = DBFile.openData(normalized);
            db = new MiniDB(normalized, directoryLock, dbFile, faultInjector);
            db.loadIndexesFromFile();

            if (dbFile.needsMigration()) {
                db.rewriteCurrentStateToV2(true);
            }

            db.walFile = WalFile.open(normalized);
            db.recoverFromWal();
            db.nextSequence = db.maxSequence + 1;
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
     * Stores {@code value} for {@code key}.
     *
     * <p>When this method returns successfully, the mutation's WAL record has been forced to stable
     * storage. The data-file append may still be awaiting a checkpoint; recovery replays the WAL if
     * needed.</p>
     */
    public void put(byte[] key, byte[] value) throws IOException {
        requireKey(key);
        requireValue(value);

        lock.writeLock().lock();
        try {
            ensureUsable();
            Entry entry = new Entry(key, value, Entry.PUT, nextSequence++);
            appendDurably(entry);
        } catch (IOException error) {
            recoveryRequired = true;
            throw error;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Returns a copy of the stored value, or {@code null} when the key does not exist. */
    public byte[] get(byte[] key) throws IOException {
        requireKey(key);

        lock.readLock().lock();
        try {
            ensureUsable();
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
            ensureUsable();
            ByteArrayKey lookupKey = new ByteArrayKey(key);
            if (!indexes.containsKey(lookupKey)) {
                return false;
            }
            Entry entry = new Entry(key, new byte[0], Entry.DELETE, nextSequence++);
            appendDurably(entry);
            return true;
        } catch (IOException error) {
            recoveryRequired = true;
            throw error;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Returns the number of currently live keys. */
    public long size() {
        lock.readLock().lock();
        try {
            ensureUsable();
            return indexes.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checkpoints all successful mutations into the data file and resets the WAL.
     *
     * <p>Successful {@code put}/{@code delete} calls are already WAL-durable. {@code sync()} is the
     * explicit checkpoint boundary that also forces the canonical data file.</p>
     */
    public void sync() throws IOException {
        lock.writeLock().lock();
        try {
            ensureUsable();
            checkpoint();
        } catch (IOException error) {
            recoveryRequired = true;
            throw error;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Rewrites only live entries into a fresh v2 data file, then checkpoints the WAL. */
    public void merge() throws IOException {
        lock.writeLock().lock();
        try {
            ensureUsable();
            rewriteCurrentStateToV2(false);
            walFile.reset();
        } catch (IOException error) {
            recoveryRequired = true;
            throw error;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void appendDurably(Entry entry) throws IOException {
        walFile.append(entry);
        walFile.sync();
        faultInjector.hit(FaultInjector.FaultPoint.AFTER_WAL_SYNC_BEFORE_DATA_APPEND);

        long offset = dbFile.append(entry);
        faultInjector.hit(FaultInjector.FaultPoint.AFTER_DATA_APPEND_BEFORE_INDEX_UPDATE);

        applyToIndex(entry, offset);
        maxSequence = Math.max(maxSequence, entry.sequence());
    }

    private void checkpoint() throws IOException {
        dbFile.sync();
        faultInjector.hit(FaultInjector.FaultPoint.AFTER_DATA_SYNC_BEFORE_WAL_RESET);
        walFile.reset();
    }

    private void recoverFromWal() throws IOException {
        long offset = walFile.dataStartOffset();
        long previousWalSequence = 0;
        boolean replayed = false;

        while (offset < walFile.size()) {
            Entry entry;
            try {
                entry = walFile.read(offset);
            } catch (WalFile.TruncatedWalEntryException truncated) {
                walFile.truncate(truncated.entryOffset());
                break;
            }

            if (entry == null) {
                break;
            }
            if (entry.sequence() <= previousWalSequence) {
                throw new CorruptDatabaseException(
                        "WAL sequence is not strictly increasing at offset " + offset);
            }
            previousWalSequence = entry.sequence();

            if (entry.sequence() > maxSequence) {
                long dataOffset = dbFile.append(entry);
                applyToIndex(entry, dataOffset);
                maxSequence = entry.sequence();
                replayed = true;
            }
            offset += entry.size();
        }

        if (replayed) {
            dbFile.sync();
        }
        walFile.reset();
    }

    private void rewriteCurrentStateToV2(boolean assignMigrationSequences) throws IOException {
        Map<ByteArrayKey, Long> rebuiltIndexes = new HashMap<>();
        Path mergePath;
        long assignedSequence = 0;
        long rewrittenMaxSequence = 0;

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
                    Entry output = assignMigrationSequences
                            ? entry.withSequence(++assignedSequence)
                            : entry;
                    long newOffset = mergeFile.append(output);
                    rebuiltIndexes.put(key, newOffset);
                    rewrittenMaxSequence = Math.max(rewrittenMaxSequence, output.sequence());
                }
                offset += entry.size();
            }
            mergeFile.sync();
        }

        Path dataPath = directory.resolve(DBFile.DATA_FILE_NAME);
        dbFile.close();
        try {
            try {
                Files.move(
                        mergePath,
                        dataPath,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(mergePath, dataPath, StandardCopyOption.REPLACE_EXISTING);
            }

            dbFile = DBFile.openData(directory);
            indexes.clear();
            indexes.putAll(rebuiltIndexes);
            maxSequence = rewrittenMaxSequence;
            nextSequence = maxSequence + 1;
        } catch (IOException moveFailure) {
            try {
                dbFile = DBFile.openData(directory);
                loadIndexesFromFile();
            } catch (IOException recoveryFailure) {
                moveFailure.addSuppressed(recoveryFailure);
            }
            throw moveFailure;
        }
    }

    private void loadIndexesFromFile() throws IOException {
        indexes.clear();
        maxSequence = 0;
        long previousSequence = 0;
        long offset = dbFile.dataStartOffset();

        while (offset < dbFile.size()) {
            Entry entry;
            try {
                entry = dbFile.read(offset);
            } catch (DBFile.TruncatedEntryException truncated) {
                dbFile.truncate(truncated.entryOffset());
                break;
            }

            if (entry == null) {
                break;
            }

            if (dbFile.formatVersion() == FileFormat.VERSION) {
                if (entry.sequence() <= previousSequence) {
                    throw new CorruptDatabaseException(
                            "data-file sequence is not strictly increasing at offset " + offset);
                }
                previousSequence = entry.sequence();
                maxSequence = entry.sequence();
            }

            applyToIndex(entry, offset);
            offset += entry.size();
        }
    }

    private void applyToIndex(Entry entry, long offset) {
        ByteArrayKey key = new ByteArrayKey(entry.key());
        if (entry.mark() == Entry.DELETE) {
            indexes.remove(key);
        } else {
            indexes.put(key, offset);
        }
    }

    private static void requireKey(byte[] key) {
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("key must not be null or empty");
        }
        if (key.length > MAX_KEY_SIZE) {
            throw new IllegalArgumentException(
                    "key exceeds max size of " + MAX_KEY_SIZE + " bytes");
        }
    }

    private static void requireValue(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }
        if (value.length > MAX_VALUE_SIZE) {
            throw new IllegalArgumentException(
                    "value exceeds max size of " + MAX_VALUE_SIZE + " bytes");
        }
    }

    private void ensureUsable() {
        if (closed) {
            throw new IllegalStateException("database is closed");
        }
        if (recoveryRequired) {
            throw new IllegalStateException(
                    "database requires close and reopen after a failed persistence operation");
        }
    }

    private void closeAfterOpenFailure(Throwable primary) {
        if (walFile != null) {
            try {
                walFile.close();
            } catch (IOException closeError) {
                primary.addSuppressed(closeError);
            }
        }
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

            IOException failure = null;
            if (!recoveryRequired && walFile != null) {
                try {
                    checkpoint();
                } catch (IOException error) {
                    recoveryRequired = true;
                    failure = error;
                }
            }

            IOException closeFailure = closeResources();
            closed = true;
            if (failure != null && closeFailure != null) {
                failure.addSuppressed(closeFailure);
            } else if (failure == null) {
                failure = closeFailure;
            }
            if (failure != null) {
                throw failure;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private IOException closeResources() {
        IOException failure = null;
        if (walFile != null) {
            try {
                walFile.close();
            } catch (IOException error) {
                failure = error;
            }
        }
        try {
            dbFile.close();
        } catch (IOException error) {
            if (failure == null) {
                failure = error;
            } else {
                failure.addSuppressed(error);
            }
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
        return failure;
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
