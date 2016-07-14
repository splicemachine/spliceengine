/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
