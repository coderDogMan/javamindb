package io.github.coderdogman.javamindb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

/** Owns immutable SSTables and resolves newest-table-wins reads. */
final class SSTableManager implements AutoCloseable {
    private final Path directory;
    private final List<SSTable> tables = new ArrayList<>();
    private long nextGeneration = 1;

    private SSTableManager(Path directory) {
        this.directory = directory;
    }

    static SSTableManager open(Path databaseDirectory) throws IOException {
        SSTableManager manager = new SSTableManager(databaseDirectory);
        Path sstableDirectory = databaseDirectory.resolve(SSTable.DIRECTORY_NAME);
        Files.createDirectories(sstableDirectory);
        try (var stream = Files.list(sstableDirectory)) {
            List<Path> paths = stream
                    .filter(path -> path.getFileName().toString().endsWith(".sst"))
                    .sorted()
                    .toList();
            for (Path path : paths) {
                SSTable table = SSTable.open(path);
                manager.tables.add(table);
                manager.nextGeneration = Math.max(manager.nextGeneration, table.generation() + 1);
            }
        }
        manager.tables.sort(Comparator.comparingLong(SSTable::generation));
        return manager;
    }

    Entry get(byte[] key) throws IOException {
        for (int i = tables.size() - 1; i >= 0; i--) {
            Entry entry = tables.get(i).get(key);
            if (entry != null) {
                return entry;
            }
        }
        return null;
    }

    long maxSequence() {
        long max = 0;
        for (SSTable table : tables) {
            max = Math.max(max, table.maxSequence());
        }
        return max;
    }

    int tableCount() {
        return tables.size();
    }

    void flush(MemTable.ImmutableMemTable memTable) throws IOException {
        if (memTable.isEmpty()) {
            return;
        }
        SSTable table = SSTable.write(directory, nextGeneration++, memTable);
        tables.add(table);
    }

    List<Entry> mergedOrderedEntries() throws IOException {
        TreeMap<ByteArrayKey, Entry> merged = new TreeMap<>();
        for (SSTable table : tables) {
            for (Entry entry : table.orderedEntries()) {
                Entry current = merged.get(new ByteArrayKey(entry.key()));
                if (current == null || entry.sequence() > current.sequence()) {
                    merged.put(new ByteArrayKey(entry.key()), entry);
                }
            }
        }
        return List.copyOf(merged.values());
    }

    void clear() throws IOException {
        IOException failure = null;
        for (SSTable table : tables) {
            try {
                table.close();
            } catch (IOException error) {
                if (failure == null) {
                    failure = error;
                } else {
                    failure.addSuppressed(error);
                }
            }
        }
        tables.clear();

        Path sstableDirectory = directory.resolve(SSTable.DIRECTORY_NAME);
        if (Files.exists(sstableDirectory)) {
            try (var stream = Files.list(sstableDirectory)) {
                for (Path path : stream.toList()) {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException error) {
                        if (failure == null) {
                            failure = error;
                        } else {
                            failure.addSuppressed(error);
                        }
                    }
                }
            }
        }
        nextGeneration = 1;
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        for (SSTable table : tables) {
            try {
                table.close();
            } catch (IOException error) {
                if (failure == null) {
                    failure = error;
                } else {
                    failure.addSuppressed(error);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
