package io.github.coderdogman.javamindb;

import java.io.IOException;

/**
 * Thrown when the data file uses a JavaMinDB format version this build does not understand.
 */
public class UnsupportedFormatException extends IOException {
    public UnsupportedFormatException(String message) {
        super(message);
    }
}
