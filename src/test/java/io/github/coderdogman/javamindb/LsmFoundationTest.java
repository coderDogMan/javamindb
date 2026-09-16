package io.github.coderdogman.javamindb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LsmFoundationTest {
    @TempDir
    Path tempDir;

    @Test
    void flushFreezesMemTableIntoImmutableSstable() throws Exception {
        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(bytes("alpha"), bytes("1"));
            db.put(bytes("beta"), bytes("2"));
            assertEquals(2, db.memTableEntryCountForTests());
            assertEquals(0, db.sstableCountForTests());

            db.flush();

            assertEquals(0, db.memTableEntryCountForTests());
            assertEquals(1, db.sstableCountForTests());
            assertArrayEquals(bytes("1"), db.get(bytes("alpha")));
            assertArrayEquals(bytes("2"), db.get(bytes("beta")));
        }
    }

    @Test
    void newerMemTableMutationOverridesOlderSstable() throws Exception {
        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(bytes("key"), bytes("old"));
            db.flush();
            db.put(bytes("key"), bytes("new"));
            assertArrayEquals(bytes("new"), db.get(bytes("key")));

            assertTrue(db.delete(bytes("key")));
            assertNull(db.get(bytes("key")), "MemTable tombstone must hide older SSTable value");
        }
    }

    @Test
    void multipleSstablesUseNewestMutationAndSurviveReopen() throws Exception {
        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(bytes("key"), bytes("v1"));
            db.flush();
            db.put(bytes("key"), bytes("v2"));
            db.put(bytes("other"), bytes("x"));
            db.flush();
            assertEquals(2, db.sstableCountForTests());
        }

        try (MiniDB reopened = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("v2"), reopened.get(bytes("key")));
            assertArrayEquals(bytes("x"), reopened.get(bytes("other")));
            assertEquals(2, reopened.sstableCountForTests());
            assertEquals(0, reopened.memTableEntryCountForTests());
        }
    }

    @Test
    void sparseIndexLookupWorksAcrossManySortedKeys() throws Exception {
        try (MiniDB db = MiniDB.open(tempDir)) {
            for (int i = 0; i < 80; i++) {
                db.put(bytes(String.format("key-%03d", i)), bytes("value-" + i));
            }
            db.flush();
            assertEquals(1, db.sstableCountForTests());

            assertArrayEquals(bytes("value-0"), db.get(bytes("key-000")));
            assertArrayEquals(bytes("value-17"), db.get(bytes("key-017")));
            assertArrayEquals(bytes("value-63"), db.get(bytes("key-063")));
            assertArrayEquals(bytes("value-79"), db.get(bytes("key-079")));
            assertNull(db.get(bytes("key-999")));
        }
    }

    @Test
    void entriesReturnsUnsignedLexicographicLiveSnapshot() throws Exception {
        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(new byte[]{(byte) 0xff}, bytes("last"));
            db.put(new byte[]{0x00, 0x01}, bytes("middle"));
            db.put(new byte[]{0x00}, bytes("first"));
            db.put(bytes("deleted"), bytes("gone"));
            db.flush();
            db.delete(bytes("deleted"));

            List<KeyValue> entries = db.entries();
            assertEquals(3, entries.size());
            assertArrayEquals(new byte[]{0x00}, entries.get(0).key());
            assertArrayEquals(new byte[]{0x00, 0x01}, entries.get(1).key());
            assertArrayEquals(new byte[]{(byte) 0xff}, entries.get(2).key());

            byte[] copy = entries.get(0).value();
            copy[0] = 'X';
            assertArrayEquals(bytes("first"), db.get(new byte[]{0x00}));
        }
    }

    @Test
    void mergeDropsStaleTablesWithoutResurrectingDeletedKeys() throws Exception {
        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(bytes("keep"), bytes("old"));
            db.put(bytes("remove"), bytes("value"));
            db.flush();
            db.put(bytes("keep"), bytes("new"));
            db.delete(bytes("remove"));
            db.flush();
            assertEquals(2, db.sstableCountForTests());

            db.merge();
            assertEquals(1, db.sstableCountForTests());
            assertArrayEquals(bytes("new"), db.get(bytes("keep")));
            assertNull(db.get(bytes("remove")));
        }

        try (MiniDB reopened = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("new"), reopened.get(bytes("keep")));
            assertNull(reopened.get(bytes("remove")));
        }
    }

    @Test
    void corruptSstablePayloadFailsClosedOnOpen() throws Exception {
        Path sstable;
        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(bytes("key"), bytes("value"));
            db.flush();
            sstable = Files.list(tempDir.resolve(SSTable.DIRECTORY_NAME))
                    .filter(path -> path.getFileName().toString().endsWith(".sst"))
                    .findFirst()
                    .orElseThrow();
        }

        try (RandomAccessFile file = new RandomAccessFile(sstable.toFile(), "rw")) {
            long payloadOffset = 32L + Entry.V2_HEADER_SIZE + bytes("key").length;
            file.seek(payloadOffset);
            int original = file.read();
            file.seek(payloadOffset);
            file.write(original ^ 0x01);
        }

        CorruptDatabaseException error =
                assertThrows(CorruptDatabaseException.class, () -> MiniDB.open(tempDir));
        assertTrue(error.getMessage().contains("CRC32C"));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
