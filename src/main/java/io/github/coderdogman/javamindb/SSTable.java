package io.github.coderdogman.javamindb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Immutable sorted table with an in-memory sparse index. */
final class SSTable implements AutoCloseable {
    static final String DIRECTORY_NAME = "sstables";
    static final int SPARSE_INDEX_INTERVAL = 16;

    private static final byte[] MAGIC = new byte[]{'J', 'M', 'I', 'N', 'S', 'S', 'T', '1'};
    private static final int VERSION = 1;
    private static final int HEADER_SIZE = 32;

    private final Path path;
    private final FileChannel channel;
    private final long generation;
    private final long maxSequence;
    private final List<SparseEntry> sparseIndex;

    private SSTable(
            Path path,
            FileChannel channel,
            long generation,
            long maxSequence,
            List<SparseEntry> sparseIndex) {
        this.path = path;
        this.channel = channel;
        this.generation = generation;
        this.maxSequence = maxSequence;
        this.sparseIndex = sparseIndex;
    }

    static SSTable write(Path directory, long generation, MemTable.ImmutableMemTable table)
            throws IOException {
        if (table.isEmpty()) {
            throw new IllegalArgumentException("cannot write an empty SSTable");
        }

        Path sstableDirectory = directory.resolve(DIRECTORY_NAME);
        Files.createDirectories(sstableDirectory);
        Path target = sstableDirectory.resolve(fileName(generation));
        Path temporary = sstableDirectory.resolve(fileName(generation) + ".tmp");
        Files.deleteIfExists(temporary);

        long maxSequence = table.entries().stream().mapToLong(Entry::sequence).max().orElseThrow();
        try (FileChannel channel = FileChannel.open(
                temporary,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ)) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            header.put(MAGIC);
            header.putInt(VERSION);
            header.putInt(HEADER_SIZE);
            header.putLong(generation);
            header.putLong(maxSequence);
            writeFully(channel, (ByteBuffer) header.flip(), 0);

            long offset = HEADER_SIZE;
            ByteArrayKey previous = null;
            for (Entry entry : table.entries()) {
                ByteArrayKey key = new ByteArrayKey(entry.key());
                if (previous != null && previous.compareTo(key) >= 0) {
                    throw new IllegalArgumentException("SSTable input must be strictly key-sorted");
                }
                byte[] encoded = entry.encodeV2();
                writeFully(channel, ByteBuffer.wrap(encoded), offset);
                offset += encoded.length;
                previous = key;
            }
            channel.force(true);
        }

        try {
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(temporary, target);
            }
        } catch (IOException moveFailure) {
            Files.deleteIfExists(temporary);
            throw moveFailure;
        }
        return open(target);
    }

    static SSTable open(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        boolean success = false;
        try {
            if (channel.size() < HEADER_SIZE) {
                throw new CorruptDatabaseException("truncated SSTable header: " + path);
            }
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            readFully(channel, header, 0);
            header.flip();
            byte[] magic = new byte[MAGIC.length];
            header.get(magic);
            if (!Arrays.equals(magic, MAGIC)) {
                throw new CorruptDatabaseException("invalid SSTable magic: " + path);
            }
            int version = header.getInt();
            int headerSize = header.getInt();
            long generation = header.getLong();
            long maxSequence = header.getLong();
            if (version != VERSION || headerSize != HEADER_SIZE || generation <= 0 || maxSequence <= 0) {
                throw new CorruptDatabaseException("invalid SSTable header: " + path);
            }

            List<SparseEntry> sparse = buildSparseIndex(channel, path);
            SSTable result = new SSTable(path, channel, generation, maxSequence, sparse);
            success = true;
            return result;
        } finally {
            if (!success) {
                channel.close();
            }
        }
    }

    Entry get(byte[] keyBytes) throws IOException {
        ByteArrayKey target = new ByteArrayKey(keyBytes);
        long offset = sparseStart(target);
        long length = channel.size();
        while (offset < length) {
            Entry entry = readEntry(offset);
            ByteArrayKey current = new ByteArrayKey(entry.key());
            int comparison = current.compareTo(target);
            if (comparison == 0) {
                return entry;
            }
            if (comparison > 0) {
                return null;
            }
            offset += entry.size();
        }
        return null;
    }

    List<Entry> orderedEntries() throws IOException {
        List<Entry> result = new ArrayList<>();
        long offset = HEADER_SIZE;
        long length = channel.size();
        while (offset < length) {
            Entry entry = readEntry(offset);
            result.add(entry);
            offset += entry.size();
        }
        return result;
    }

    long generation() {
        return generation;
    }

    long maxSequence() {
        return maxSequence;
    }

    Path path() {
        return path;
    }

    private long sparseStart(ByteArrayKey target) {
        if (sparseIndex.isEmpty()) {
            return HEADER_SIZE;
        }
        int low = 0;
        int high = sparseIndex.size() - 1;
        int floor = -1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            SparseEntry candidate = sparseIndex.get(mid);
            if (candidate.key().compareTo(target) <= 0) {
                floor = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return floor < 0 ? HEADER_SIZE : sparseIndex.get(floor).offset();
    }

    private Entry readEntry(long offset) throws IOException {
        long remaining = channel.size() - offset;
        if (remaining < Entry.V2_HEADER_SIZE) {
            throw new CorruptDatabaseException("truncated SSTable record header at offset " + offset);
        }
        ByteBuffer header = ByteBuffer.allocate(Entry.V2_HEADER_SIZE);
        readFully(channel, header, offset);
        Entry metadata = Entry.decodeV2Header(header.array(), offset);
        if (metadata.size() > remaining) {
            throw new CorruptDatabaseException("truncated SSTable record at offset " + offset);
        }
        long cursor = offset + Entry.V2_HEADER_SIZE;
        byte[] key = new byte[metadata.keySize()];
        readFully(channel, ByteBuffer.wrap(key), cursor);
        cursor += key.length;
        byte[] value = new byte[metadata.valueSize()];
        if (value.length > 0) {
            readFully(channel, ByteBuffer.wrap(value), cursor);
        }
        return metadata.withPayload(key, value, offset);
    }

    private static List<SparseEntry> buildSparseIndex(FileChannel channel, Path path) throws IOException {
        List<SparseEntry> sparse = new ArrayList<>();
        long offset = HEADER_SIZE;
        long length = channel.size();
        int record = 0;
        ByteArrayKey previous = null;
        long previousSequence = 0;
        while (offset < length) {
            if (length - offset < Entry.V2_HEADER_SIZE) {
                throw new CorruptDatabaseException("truncated SSTable record header: " + path);
            }
            ByteBuffer header = ByteBuffer.allocate(Entry.V2_HEADER_SIZE);
            readFully(channel, header, offset);
            Entry metadata = Entry.decodeV2Header(header.array(), offset);
            if (metadata.size() > length - offset) {
                throw new CorruptDatabaseException("truncated SSTable record: " + path);
            }
            byte[] keyBytes = new byte[metadata.keySize()];
            readFully(channel, ByteBuffer.wrap(keyBytes), offset + Entry.V2_HEADER_SIZE);
            byte[] value = new byte[metadata.valueSize()];
            if (value.length > 0) {
                readFully(channel, ByteBuffer.wrap(value), offset + Entry.V2_HEADER_SIZE + keyBytes.length);
            }
            Entry entry = metadata.withPayload(keyBytes, value, offset);
            ByteArrayKey key = new ByteArrayKey(entry.key());
            if (previous != null && previous.compareTo(key) >= 0) {
                throw new CorruptDatabaseException("SSTable keys are not strictly sorted: " + path);
            }
            if (entry.sequence() <= 0 || entry.sequence() < previousSequence) {
                throw new CorruptDatabaseException("invalid SSTable sequence ordering: " + path);
            }
            if (record % SPARSE_INDEX_INTERVAL == 0) {
                sparse.add(new SparseEntry(key, offset));
            }
            previous = key;
            previousSequence = entry.sequence();
            offset += entry.size();
            record++;
        }
        return List.copyOf(sparse);
    }

    private static String fileName(long generation) {
        return String.format("sst-%020d.sst", generation);
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        long cursor = position;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, cursor);
            if (read < 0) {
                throw new CorruptDatabaseException("unexpected end of SSTable at offset " + cursor);
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

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private record SparseEntry(ByteArrayKey key, long offset) {
    }
}
