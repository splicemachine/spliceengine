/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * A testing probe that, after being set, blocks until unset.
 */
public class BlockingProbe {

    private static final long MILLIS_TO_BLOCK = 100L;
    private static TimeUnit MILLIS = TimeUnit.MILLISECONDS;

    private static volatile boolean blockPreSplit;
    private static volatile boolean blockPostSplit;

    private static volatile boolean blockPreCompact;
    private static volatile boolean blockPostCompact;

    private static volatile boolean blockPreFlush;
    private static volatile boolean blockPostFlush;

    // =============================================================================
    // Setters
    // =============================================================================

    public static void setBlockPreCompact(boolean blockPreCompact) {
        BlockingProbe.blockPreCompact = blockPreCompact;
    }

    public static void setBlockPostCompact(boolean blockPostCompact) {
        BlockingProbe.blockPostCompact = blockPostCompact;
    }

    public static void setBlockPreFlush(boolean blockPreFlush) {
        BlockingProbe.blockPreFlush = blockPreFlush;
    }

    public static void setBlockPostFlush(boolean blockPostFlush) {
        BlockingProbe.blockPostFlush = blockPostFlush;
    }

    public static void setBlockPreSplit(boolean blockPreSplit) {
        BlockingProbe.blockPreSplit = blockPreSplit;
    }

    public static void setBlockPostSplit(boolean blockPostSplit) {
        BlockingProbe.blockPostSplit = blockPostSplit;
    }

    public static boolean isBlockPostCompact() {
        return blockPostCompact;
    }

    public static boolean isBlockPostFlush() {
        return blockPostFlush;
    }

    public static boolean isBlockPostSplit() {
        return blockPostSplit;
    }

    public static boolean isBlockPreCompact() {
        return blockPreCompact;
    }

    public static boolean isBlockPreFlush() {
        return blockPreFlush;
    }

    public static boolean isBlockPreSplit() {
        return blockPreSplit;
    }

    // =============================================================================
    // Probe Methods
    // =============================================================================
    public static void blockPostCompact() {
        while (blockPostCompact) {
            LockSupport.parkNanos(MILLIS.toNanos(MILLIS_TO_BLOCK));
        }
    }

    public static void blockPostFlush() {
        while (blockPostFlush) {
           LockSupport.parkNanos(MILLIS.toNanos(MILLIS_TO_BLOCK));
        }
    }

    public static void blockPostSplit() {
       while (blockPostSplit) {
           LockSupport.parkNanos(MILLIS.toNanos(MILLIS_TO_BLOCK));
       }
    }

    public static void blockPreCompact() {
       while (blockPreCompact) {
           LockSupport.parkNanos(MILLIS.toNanos(MILLIS_TO_BLOCK));
       }
    }

    public static void blockPreFlush() {
       while (blockPreFlush) {
           LockSupport.parkNanos(MILLIS.toNanos(MILLIS_TO_BLOCK));
       }
    }

    public static void blockPreSplit() {
       while (blockPreSplit) {
           LockSupport.parkNanos(MILLIS.toNanos(MILLIS_TO_BLOCK));
       }
    }

}
