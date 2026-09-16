package io.github.coderdogman.javamindb;

import java.io.IOException;

/**
 * Thrown when JavaMinDB can identify an invalid or internally inconsistent on-disk structure.
 */
public class CorruptDatabaseException extends IOException {
    public CorruptDatabaseException(String message) {
        super(message);
    }

    public CorruptDatabaseException(String message, Throwable cause) {
        super(message, cause);
    }
}
