package io.github.coderdogman.javamindb;

import java.io.IOException;

@FunctionalInterface
interface FaultInjector {
    FaultInjector NONE = point -> {
    };

    void hit(FaultPoint point) throws IOException;

    enum FaultPoint {
        AFTER_WAL_SYNC_BEFORE_DATA_APPEND,
        AFTER_DATA_APPEND_BEFORE_INDEX_UPDATE,
        AFTER_DATA_SYNC_BEFORE_WAL_RESET
    }
}
