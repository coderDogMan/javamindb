package io.github.coderdogman.javamindb;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class DirectoryLock implements AutoCloseable {
    static final String LOCK_FILE_NAME = ".javamindb.lock";

    private final FileChannel channel;
    private final FileLock lock;

    private DirectoryLock(FileChannel channel, FileLock lock) {
        this.channel = channel;
        this.lock = lock;
    }

    static DirectoryLock acquire(Path directory) throws IOException {
        Path lockPath = directory.resolve(LOCK_FILE_NAME);
        FileChannel channel = FileChannel.open(lockPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        try {
            FileLock lock;
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException error) {
                throw new DatabaseLockedException("database directory is already open: " + directory, error);
            }
            if (lock == null) {
                throw new DatabaseLockedException("database directory is already locked: " + directory);
            }
            return new DirectoryLock(channel, lock);
        } catch (IOException | RuntimeException error) {
            channel.close();
            throw error;
        }
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        try {
            if (lock.isValid()) {
                lock.release();
            }
        } catch (IOException error) {
            failure = error;
        }
        try {
            channel.close();
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
    }
}
