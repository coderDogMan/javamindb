package io.github.coderdogman.javamindb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/** Mutable ordered write buffer. Only the latest mutation for each key is retained. */
final class MemTable {
    private final NavigableMap<ByteArrayKey, Entry> entries = new TreeMap<>();
    private long approximateBytes;

    void put(Entry entry) {
        ByteArrayKey key = new ByteArrayKey(entry.key());
        Entry previous = entries.put(key, entry);
        if (previous != null) {
            approximateBytes -= estimate(previous);
        }
        approximateBytes += estimate(entry);
    }

    Entry get(byte[] key) {
        return entries.get(new ByteArrayKey(key));
    }

    long approximateBytes() {
        return approximateBytes;
    }

    boolean isEmpty() {
        return entries.isEmpty();
    }

    int size() {
        return entries.size();
    }

    ImmutableMemTable freeze() {
        return new ImmutableMemTable(new ArrayList<>(entries.values()), approximateBytes);
    }

    void clear() {
        entries.clear();
        approximateBytes = 0;
    }

    List<Entry> orderedEntries() {
        return Collections.unmodifiableList(new ArrayList<>(entries.values()));
    }

    private static long estimate(Entry entry) {
        return (long) Entry.V2_HEADER_SIZE + entry.keySize() + entry.valueSize();
    }

    static final class ImmutableMemTable {
        private final List<Entry> entries;
        private final long approximateBytes;

        ImmutableMemTable(List<Entry> entries, long approximateBytes) {
            this.entries = List.copyOf(entries);
            this.approximateBytes = approximateBytes;
        }

        List<Entry> entries() {
            return entries;
        }

        long approximateBytes() {
            return approximateBytes;
        }

        boolean isEmpty() {
            return entries.isEmpty();
        }
    }
}
