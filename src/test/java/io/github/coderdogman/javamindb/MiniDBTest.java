package io.github.coderdogman.javamindb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class MiniDBTest {
    @TempDir
    Path tempDir;

    @Test
    void putGetOverwriteDeleteAndEmptyValueHaveDefinedSemantics() throws Exception {
        byte[] key = bytes("name");

        try (MiniDB db = MiniDB.open(tempDir)) {
            assertNull(db.get(key));
            assertFalse(db.delete(key));

            db.put(key, bytes("Alice"));
            assertArrayEquals(bytes("Alice"), db.get(key));
            assertEquals(1, db.size());

            db.put(key, bytes("Bob"));
            assertArrayEquals(bytes("Bob"), db.get(key));
            assertEquals(1, db.size());

            db.put(bytes("empty"), new byte[0]);
            assertArrayEquals(new byte[0], db.get(bytes("empty")));
            assertEquals(2, db.size());

            assertTrue(db.delete(key));
            assertNull(db.get(key));
            assertFalse(db.delete(key));
            assertEquals(1, db.size());
        }
    }

    @Test
    void inputAndOutputByteArraysAreIsolatedFromStorage() throws Exception {
        byte[] key = bytes("key");
        byte[] value = bytes("value");

        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(key, value);
            key[0] = 'X';
            value[0] = 'X';

            byte[] stored = db.get(bytes("key"));
            assertArrayEquals(bytes("value"), stored);
            stored[0] = 'Y';
            assertArrayEquals(bytes("value"), db.get(bytes("key")));
        }
    }

    @Test
    void indexIsRebuiltAfterReopen() throws Exception {
        byte[] first = bytes("first");
        byte[] second = bytes("second");

        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(first, bytes("v1"));
            db.put(second, bytes("v2"));
            db.put(first, bytes("v1-new"));
            db.delete(second);
            db.sync();
        }

        try (MiniDB reopened = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("v1-new"), reopened.get(first));
            assertNull(reopened.get(second));
            assertEquals(1, reopened.size());
        }
    }

    @Test
    void mergeKeepsOnlyLiveValuesAndPreservesV1Header() throws Exception {
        byte[] alpha = bytes("alpha");
        byte[] beta = bytes("beta");
        byte[] gamma = bytes("gamma");

        long beforeMerge;
        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(alpha, bytes("1"));
            db.put(alpha, bytes("2"));
            db.put(beta, bytes("delete-me"));
            db.delete(beta);
            db.put(gamma, bytes("3"));

            beforeMerge = Files.size(dataFile());
            db.merge();

            assertArrayEquals(bytes("2"), db.get(alpha));
            assertNull(db.get(beta));
            assertArrayEquals(bytes("3"), db.get(gamma));
            assertEquals(2, db.size());
        }

        long afterMerge = Files.size(dataFile());
        assertTrue(afterMerge < beforeMerge, "merge should remove stale records");
        assertFalse(Files.exists(tempDir.resolve(DBFile.MERGE_FILE_NAME)));
        assertV1Header(dataFile());

        try (MiniDB reopened = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("2"), reopened.get(alpha));
            assertNull(reopened.get(beta));
            assertArrayEquals(bytes("3"), reopened.get(gamma));
        }
    }

    @Test
    void databaseFileStartsWithVersionedHeader() throws Exception {
        try (MiniDB ignored = MiniDB.open(tempDir)) {
            // Creation is sufficient.
        }
        assertV1Header(dataFile());
        assertEquals(FileFormat.FILE_HEADER_SIZE, Files.size(dataFile()));
    }

    @Test
    void phase0HeaderlessFileIsMigratedToV1() throws Exception {
        byte[] legacy = concat(
                new Entry(bytes("alpha"), bytes("old"), Entry.PUT).encode(),
                new Entry(bytes("alpha"), bytes("new"), Entry.PUT).encode(),
                new Entry(bytes("deleted"), bytes("gone"), Entry.PUT).encode(),
                new Entry(bytes("deleted"), new byte[0], Entry.DELETE).encode());
        Files.write(dataFile(), legacy, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        try (MiniDB db = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("new"), db.get(bytes("alpha")));
            assertNull(db.get(bytes("deleted")));
            assertEquals(1, db.size());
        }

        assertV1Header(dataFile());
        assertTrue(Files.size(dataFile()) < legacy.length + FileFormat.FILE_HEADER_SIZE,
                "legacy migration should rewrite only live entries");
    }

    @Test
    void incompleteFinalRecordIsDiscardedOnOpen() throws Exception {
        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(bytes("stable"), bytes("keep"));
            db.put(bytes("torn"), bytes("discard-me"));
            db.sync();
        }

        long completeLength = Files.size(dataFile());
        try (RandomAccessFile file = new RandomAccessFile(dataFile().toFile(), "rw")) {
            file.setLength(completeLength - 3);
        }

        try (MiniDB reopened = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("keep"), reopened.get(bytes("stable")));
            assertNull(reopened.get(bytes("torn")));
            assertEquals(1, reopened.size());
        }
        assertTrue(Files.size(dataFile()) < completeLength);
    }

    @Test
    void malformedEntryHeaderFailsClosedAsCorruption() throws Exception {
        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(bytes("key"), bytes("value"));
            db.sync();
        }

        try (RandomAccessFile file = new RandomAccessFile(dataFile().toFile(), "rw")) {
            file.seek(FileFormat.FILE_HEADER_SIZE + 8L);
            file.writeShort(99);
        }

        CorruptDatabaseException error = assertThrows(
                CorruptDatabaseException.class, () -> MiniDB.open(tempDir));
        assertTrue(error.getMessage().contains("operation"));
    }

    @Test
    void truncatedVersionedFileHeaderFailsClosed() throws Exception {
        Files.write(dataFile(), Arrays.copyOf(FileFormat.MAGIC, 4),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        CorruptDatabaseException error = assertThrows(
                CorruptDatabaseException.class, () -> MiniDB.open(tempDir));
        assertTrue(error.getMessage().contains("file header"));
    }

    @Test
    void unsupportedFileVersionIsRejected() throws Exception {
        try (MiniDB ignored = MiniDB.open(tempDir)) {
            // Create a v1 file first.
        }

        try (RandomAccessFile file = new RandomAccessFile(dataFile().toFile(), "rw")) {
            file.seek(FileFormat.MAGIC.length);
            file.writeInt(999);
        }

        assertThrows(UnsupportedFormatException.class, () -> MiniDB.open(tempDir));
    }

    @Test
    void directoryCanOnlyBeOpenedByOneInstanceAtATime() throws Exception {
        try (MiniDB first = MiniDB.open(tempDir)) {
            assertThrows(DatabaseLockedException.class, () -> MiniDB.open(tempDir));
        }

        try (MiniDB reopened = MiniDB.open(tempDir)) {
            assertEquals(0, reopened.size());
        }
    }

    @Test
    void binaryKeysDoNotDependOnDefaultCharset() throws Exception {
        byte[] keyA = new byte[]{(byte) 0x80};
        byte[] keyB = new byte[]{(byte) 0x81};

        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(keyA, bytes("A"));
            db.put(keyB, bytes("B"));

            assertArrayEquals(bytes("A"), db.get(keyA));
            assertArrayEquals(bytes("B"), db.get(keyB));
        }
    }

    @Test
    void rejectsInvalidArguments() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> MiniDB.open((Path) null));
        assertThrows(IllegalArgumentException.class, () -> MiniDB.open((String) null));

        try (MiniDB db = MiniDB.open(tempDir)) {
            assertThrows(IllegalArgumentException.class, () -> db.put(null, bytes("x")));
            assertThrows(IllegalArgumentException.class, () -> db.put(new byte[0], bytes("x")));
            assertThrows(IllegalArgumentException.class, () -> db.put(bytes("k"), null));
            assertThrows(IllegalArgumentException.class, () -> db.get(new byte[0]));
            assertThrows(IllegalArgumentException.class, () -> db.delete(new byte[0]));
        }
    }

    @Test
    void operationsAfterCloseFailWithIllegalStateExceptionAndCloseIsIdempotent() throws Exception {
        MiniDB db = MiniDB.open(tempDir);
        db.close();
        db.close();

        assertThrows(IllegalStateException.class, db::size);
        assertThrows(IllegalStateException.class, () -> db.get(bytes("x")));
        assertThrows(IllegalStateException.class, () -> db.put(bytes("x"), bytes("y")));
        assertThrows(IllegalStateException.class, () -> db.delete(bytes("x")));
        assertThrows(IllegalStateException.class, db::sync);
        assertThrows(IllegalStateException.class, db::merge);
    }

    private Path dataFile() {
        return tempDir.resolve(DBFile.DATA_FILE_NAME);
    }

    private static void assertV1Header(Path path) throws Exception {
        byte[] bytes = Files.readAllBytes(path);
        assertTrue(bytes.length >= FileFormat.FILE_HEADER_SIZE);
        assertArrayEquals(FileFormat.MAGIC, Arrays.copyOfRange(bytes, 0, FileFormat.MAGIC.length));
        ByteBuffer header = ByteBuffer.wrap(bytes, FileFormat.MAGIC.length, 8);
        assertEquals(FileFormat.VERSION, header.getInt());
        assertEquals(FileFormat.FILE_HEADER_SIZE, header.getInt());
    }

    private static byte[] concat(byte[]... chunks) {
        int size = Arrays.stream(chunks).mapToInt(chunk -> chunk.length).sum();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (byte[] chunk : chunks) {
            buffer.put(chunk);
        }
        return buffer.array();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
