package io.github.coderdogman.javamindb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
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
        try (MiniDB db = MiniDB.open(tempDir)) {
            assertNull(db.get(bytes("name")));
            assertFalse(db.delete(bytes("name")));

            db.put(bytes("name"), bytes("Alice"));
            assertArrayEquals(bytes("Alice"), db.get(bytes("name")));

            db.put(bytes("name"), bytes("Bob"));
            assertArrayEquals(bytes("Bob"), db.get(bytes("name")));
            assertEquals(1, db.size());

            db.put(bytes("empty"), new byte[0]);
            assertArrayEquals(new byte[0], db.get(bytes("empty")));
            assertTrue(db.delete(bytes("name")));
            assertNull(db.get(bytes("name")));
            assertEquals(1, db.size());
        }
    }

    @Test
    void successfulWriteSurvivesCrashAfterWalSyncBeforeDataAppend() throws Exception {
        FaultInjector injector = point -> {
            if (point == FaultInjector.FaultPoint.AFTER_WAL_SYNC_BEFORE_DATA_APPEND) {
                throw new IOException("simulated crash after WAL fsync");
            }
        };

        MiniDB db = MiniDB.open(tempDir, injector);
        assertThrows(IOException.class, () -> db.put(bytes("durable"), bytes("yes")));
        db.close();

        try (MiniDB recovered = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("yes"), recovered.get(bytes("durable")));
            assertEquals(1, recovered.size());
        }
        assertEquals(16, Files.size(walFile()));
    }

    @Test
    void recoveryDoesNotDuplicateRecordAlreadyAppendedToData() throws Exception {
        FaultInjector injector = point -> {
            if (point == FaultInjector.FaultPoint.AFTER_DATA_APPEND_BEFORE_INDEX_UPDATE) {
                throw new IOException("simulated crash after data append");
            }
        };

        MiniDB db = MiniDB.open(tempDir, injector);
        assertThrows(IOException.class, () -> db.put(bytes("k"), bytes("v")));
        db.close();

        long before = Files.size(dataFile());
        try (MiniDB recovered = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("v"), recovered.get(bytes("k")));
            assertEquals(1, recovered.size());
        }
        assertEquals(before, Files.size(dataFile()),
                "WAL replay must skip a sequence already present in the data file");
    }

    @Test
    void crashAfterDataSyncBeforeWalResetRecoversIdempotently() throws Exception {
        FaultInjector injector = point -> {
            if (point == FaultInjector.FaultPoint.AFTER_DATA_SYNC_BEFORE_WAL_RESET) {
                throw new IOException("simulated checkpoint crash");
            }
        };

        MiniDB db = MiniDB.open(tempDir, injector);
        db.put(bytes("k"), bytes("v"));
        assertThrows(IOException.class, db::sync);
        db.close();

        try (MiniDB recovered = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("v"), recovered.get(bytes("k")));
        }
        assertEquals(16, Files.size(walFile()));
    }

    @Test
    void partialDataTailIsRepairedThenRecoveredFromWal() throws Exception {
        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(bytes("stable"), bytes("keep"));
            db.sync();
        }

        FaultInjector injector = point -> {
            if (point == FaultInjector.FaultPoint.AFTER_DATA_APPEND_BEFORE_INDEX_UPDATE) {
                throw new IOException("simulated crash");
            }
        };
        MiniDB db = MiniDB.open(tempDir, injector);
        assertThrows(IOException.class, () -> db.put(bytes("torn"), bytes("recover-me")));
        db.close();

        try (RandomAccessFile file = new RandomAccessFile(dataFile().toFile(), "rw")) {
            file.setLength(file.length() - 3);
        }

        try (MiniDB recovered = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("keep"), recovered.get(bytes("stable")));
            assertArrayEquals(bytes("recover-me"), recovered.get(bytes("torn")));
            assertEquals(2, recovered.size());
        }
    }

    @Test
    void truncatedWalTailIsDiscardedWithoutDamagingCheckpointedData() throws Exception {
        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(bytes("stable"), bytes("value"));
            db.sync();
        }

        Files.write(walFile(), new byte[]{1, 2, 3, 4, 5}, StandardOpenOption.APPEND);

        try (MiniDB recovered = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("value"), recovered.get(bytes("stable")));
        }
        assertEquals(16, Files.size(walFile()));
    }

    @Test
    void dataPayloadBitFlipIsDetectedByCrc32c() throws Exception {
        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(bytes("key"), bytes("value"));
            db.sync();
        }

        try (RandomAccessFile file = new RandomAccessFile(dataFile().toFile(), "rw")) {
            long position = FileFormat.FILE_HEADER_SIZE + Entry.V2_HEADER_SIZE + bytes("key").length;
            file.seek(position);
            int original = file.read();
            file.seek(position);
            file.write(original ^ 0x01);
        }

        CorruptDatabaseException error =
                assertThrows(CorruptDatabaseException.class, () -> MiniDB.open(tempDir));
        assertTrue(error.getMessage().contains("CRC32C"));
    }

    @Test
    void walPayloadBitFlipIsDetectedByCrc32c() throws Exception {
        FaultInjector injector = point -> {
            if (point == FaultInjector.FaultPoint.AFTER_WAL_SYNC_BEFORE_DATA_APPEND) {
                throw new IOException("leave durable WAL record");
            }
        };
        MiniDB db = MiniDB.open(tempDir, injector);
        assertThrows(IOException.class, () -> db.put(bytes("key"), bytes("value")));
        db.close();

        try (RandomAccessFile file = new RandomAccessFile(walFile().toFile(), "rw")) {
            long position = 16L + Entry.V2_HEADER_SIZE + bytes("key").length;
            file.seek(position);
            int original = file.read();
            file.seek(position);
            file.write(original ^ 0x01);
        }

        assertThrows(CorruptDatabaseException.class, () -> MiniDB.open(tempDir));
    }

    @Test
    void v1FileIsMigratedToChecksummedV2() throws Exception {
        byte[] v1 = concat(
                FileFormat.header(FileFormat.V1),
                encodeV1(bytes("alpha"), bytes("old"), Entry.PUT),
                encodeV1(bytes("alpha"), bytes("new"), Entry.PUT),
                encodeV1(bytes("deleted"), bytes("gone"), Entry.PUT),
                encodeV1(bytes("deleted"), new byte[0], Entry.DELETE));
        Files.write(dataFile(), v1, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        try (MiniDB db = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("new"), db.get(bytes("alpha")));
            assertNull(db.get(bytes("deleted")));
            assertEquals(1, db.size());
        }

        assertCurrentHeader(dataFile());
        try (MiniDB db = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("new"), db.get(bytes("alpha")));
        }
    }

    @Test
    void phase0HeaderlessFileIsMigratedToV2() throws Exception {
        byte[] legacy = concat(
                encodeV1(bytes("alpha"), bytes("old"), Entry.PUT),
                encodeV1(bytes("alpha"), bytes("new"), Entry.PUT));
        Files.write(dataFile(), legacy, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        try (MiniDB db = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("new"), db.get(bytes("alpha")));
        }

        assertCurrentHeader(dataFile());
    }

    @Test
    void mergePreservesLiveStateAndCurrentFormat() throws Exception {
        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(bytes("alpha"), bytes("1"));
            db.put(bytes("alpha"), bytes("2"));
            db.put(bytes("beta"), bytes("delete"));
            db.delete(bytes("beta"));
            db.put(bytes("gamma"), bytes("3"));

            long before = Files.size(dataFile());
            db.merge();

            assertArrayEquals(bytes("2"), db.get(bytes("alpha")));
            assertNull(db.get(bytes("beta")));
            assertArrayEquals(bytes("3"), db.get(bytes("gamma")));
            assertTrue(Files.size(dataFile()) < before);
        }

        assertCurrentHeader(dataFile());
        assertEquals(16, Files.size(walFile()));
    }

    @Test
    void gracefulCloseCheckpointsAndResetsWal() throws Exception {
        try (MiniDB db = MiniDB.open(tempDir)) {
            db.put(bytes("a"), bytes("1"));
            assertTrue(Files.size(walFile()) > 16);
        }
        assertEquals(16, Files.size(walFile()));

        try (MiniDB reopened = MiniDB.open(tempDir)) {
            assertArrayEquals(bytes("1"), reopened.get(bytes("a")));
        }
    }

    @Test
    void inputAndOutputByteArraysAreIsolated() throws Exception {
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
    void directoryCanOnlyBeOpenedByOneInstanceAtATime() throws Exception {
        try (MiniDB first = MiniDB.open(tempDir)) {
            assertThrows(DatabaseLockedException.class, () -> MiniDB.open(tempDir));
        }
        try (MiniDB reopened = MiniDB.open(tempDir)) {
            assertEquals(0, reopened.size());
        }
    }

    @Test
    void unsupportedFutureDataVersionIsRejected() throws Exception {
        Files.write(dataFile(), FileFormat.header(999),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        assertThrows(UnsupportedFormatException.class, () -> MiniDB.open(tempDir));
    }

    @Test
    void operationsAfterCloseFailAndCloseIsIdempotent() throws Exception {
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

    private Path walFile() {
        return tempDir.resolve(WalFile.WAL_FILE_NAME);
    }

    private static void assertCurrentHeader(Path path) throws Exception {
        byte[] bytes = Files.readAllBytes(path);
        assertTrue(bytes.length >= FileFormat.FILE_HEADER_SIZE);
        assertArrayEquals(FileFormat.MAGIC,
                Arrays.copyOfRange(bytes, 0, FileFormat.MAGIC.length));
        ByteBuffer header = ByteBuffer.wrap(bytes, FileFormat.MAGIC.length, 8);
        assertEquals(FileFormat.VERSION, header.getInt());
        assertEquals(FileFormat.FILE_HEADER_SIZE, header.getInt());
    }

    private static byte[] encodeV1(byte[] key, byte[] value, short mark) {
        ByteBuffer buffer = ByteBuffer.allocate(Entry.V1_HEADER_SIZE + key.length + value.length);
        buffer.putInt(key.length);
        buffer.putInt(value.length);
        buffer.putShort(mark);
        buffer.put(key);
        buffer.put(value);
        return buffer.array();
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
