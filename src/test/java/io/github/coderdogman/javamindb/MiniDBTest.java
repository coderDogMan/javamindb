package io.github.coderdogman.javamindb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class MiniDBTest {
    @TempDir
    Path tempDir;

    @Test
    void putGetAndDeleteWork() throws Exception {
        byte[] key = bytes("name");
        byte[] value = bytes("Alice");

        try (MiniDB db = MiniDB.open(tempDir)) {
            assertNull(db.get(key));

            db.put(key, value);
            assertArrayEquals(value, db.get(key));
            assertEquals(1, db.size());

            assertTrue(db.delete(key));
            assertNull(db.get(key));
            assertEquals(0, db.size());
            assertFalse(db.delete(key));
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
        }

        try (MiniDB reopened = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("v1-new"), reopened.get(first));
            assertNull(reopened.get(second));
            assertEquals(1, reopened.size());
        }
    }

    @Test
    void mergeKeepsOnlyLiveValuesAndSurvivesReopen() throws Exception {
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

            beforeMerge = Files.size(tempDir.resolve(DBFile.DATA_FILE_NAME));
            db.merge();

            assertArrayEquals(bytes("2"), db.get(alpha));
            assertNull(db.get(beta));
            assertArrayEquals(bytes("3"), db.get(gamma));
            assertEquals(2, db.size());
        }

        long afterMerge = Files.size(tempDir.resolve(DBFile.DATA_FILE_NAME));
        assertTrue(afterMerge < beforeMerge, "merge should remove stale records");
        assertFalse(Files.exists(tempDir.resolve(DBFile.MERGE_FILE_NAME)));

        try (MiniDB reopened = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("2"), reopened.get(alpha));
            assertNull(reopened.get(beta));
            assertArrayEquals(bytes("3"), reopened.get(gamma));
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
    void rejectsInvalidKeysAndNullValues() throws Exception {
        try (MiniDB db = MiniDB.open(tempDir)) {
            assertThrows(IllegalArgumentException.class, () -> db.put(null, bytes("x")));
            assertThrows(IllegalArgumentException.class, () -> db.put(new byte[0], bytes("x")));
            assertThrows(IllegalArgumentException.class, () -> db.put(bytes("k"), null));
            assertThrows(IllegalArgumentException.class, () -> db.get(new byte[0]));
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
