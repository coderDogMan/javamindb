package io.github.coderdogman.javamindb;

import java.io.IOException;

/**
 * Thrown when another JavaMinDB instance already owns the database directory.
 */
public class DatabaseLockedException extends IOException {
    public DatabaseLockedException(String message) {
        super(message);
    }

    public DatabaseLockedException(String message, Throwable cause) {
        super(message, cause);
    }
}
