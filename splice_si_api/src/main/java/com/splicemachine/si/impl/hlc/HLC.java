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

package com.splicemachine.si.impl.hlc;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * Please see https://www.cse.buffalo.edu/tech-reports/2014-04.pdf
 * if you are interested in the underpinnings of the hybrid logical clock.
 *
 */
public class HLC {
    public static final int hlcNumBitsToShift = 12;
    public static final int hlcLogicalBitsMask = (1 << hlcNumBitsToShift) - 1;
    AtomicLong atomicHLC = new AtomicLong(physicalAndLogicalToHLC(0l, 0l));

    public long sendOrLocalEvent() {
        long currentHLC;
        long returnHLC;
        while (true) {
            currentHLC = atomicHLC.get();
            long[] hlc = HLCToPhysicalAndLogical(currentHLC);
            long logical = Math.max(hlc[0], System.currentTimeMillis());
            if (logical == hlc[0])
                hlc[1]++;
            else {
                hlc[0] = logical;
                hlc[1] = 0l;
            }
            returnHLC = physicalAndLogicalToHLC(hlc[0],hlc[1]);
            if (atomicHLC.compareAndSet(currentHLC,returnHLC))
                return returnHLC;
        }
    }

    public long receiveEvent(long message) {
        long currentHLC;
        long returnHLC;
        long[] messageHLC = HLCToPhysicalAndLogical(message);
        while (true) {
            currentHLC = atomicHLC.get();
            long[] hlc = HLCToPhysicalAndLogical(atomicHLC.get());
            long logical = Math.max(hlc[0],Math.max(messageHLC[0], System.currentTimeMillis()));
            if (logical == hlc[0] && logical==messageHLC[0])
                hlc[1] = Math.max(hlc[0],messageHLC[0]) +1;
            else if (logical == hlc[0])
                hlc[1]++;
            else if (logical == messageHLC[0]) {
                hlc[0] = logical;
                hlc[1] = messageHLC[1]+1;
            }
            else {
                hlc[0] = logical;
                hlc[1] = 0;
            }
            returnHLC = physicalAndLogicalToHLC(hlc[0],hlc[1]);
            if (atomicHLC.compareAndSet(currentHLC,returnHLC))
                return returnHLC;
        }
    }

    /**
     * Converts the provided timestamp, in the provided unit, to the HybridTime timestamp
     * format. Logical bits are set to 0.
     *
     * @param timestamp the value of the timestamp, must be greater than 0
     * @param timeUnit  the time unit of the timestamp
     * @throws IllegalArgumentException if the timestamp is less than 0
     */
    public static long clockTimestampToHLC(long timestamp, TimeUnit timeUnit) {
        if (timestamp < 0) {
            throw new IllegalArgumentException("Timestamp cannot be less than 0");
        }
        long timestampInMicros = TimeUnit.MICROSECONDS.convert(timestamp, timeUnit);
        return timestampInMicros << hlcNumBitsToShift;
    }

    /**
     * Extracts the physical and logical values from an HT timestamp.
     *
     * @param htTimestamp the encoded HT timestamp
     * @return a pair of {physical, logical} long values in an array
     */
    public static long[] HLCToPhysicalAndLogical(long htTimestamp) {
        long timestampInMicros = htTimestamp >> hlcNumBitsToShift;
        long logicalValues = htTimestamp & hlcLogicalBitsMask;
        return new long[] {timestampInMicros, logicalValues};
    }

    /**
     * Encodes separate physical and logical components into a single HT timestamp
     *
     * @param physical the physical component, in microseconds
     * @param logical  the logical component
     * @return an encoded HT timestamp
     */
    public static long physicalAndLogicalToHLC(long physical, long logical) {
        return (physical << hlcNumBitsToShift) + logical;
    }

}
