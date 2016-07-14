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

package com.splicemachine.derby.impl.sql.execute.operations.window;


import java.io.Serializable;

import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This represent the frame definition for a given window function as a representation
 * of the parsed frame clause from a window over() clause.
 *
 * @author Jeff Cunningham
 *         Date: 7/10/14
 */
public class FrameDefinition {

    public enum FrameMode { ROWS, RANGE }

    public enum Frame {
        UNBOUNDED_PRECEDING,
        PRECEDING,
        CURRENT_ROW,
        FOLLOWING,
        UNBOUNDED_FOLLOWING
    }

    public static class FrameType implements Serializable {
        private final Frame frame;
        private final long value;

        FrameType(int frame, long value) {
            // default access to allow testing
            this.frame = Frame.values()[frame];
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        public Frame getFrame() {
            return frame;
        }

        @Override
        public String toString() {
            return value + " " + frame;
        }
    }

    private final FrameMode frameMode;
    private final FrameType frameStart;
    private final FrameType frameEnd;

    /**
     * Construct a window frame definition
     * @param frameMode frame mode [RANGE | ROWS]
     * @param frameStart start of the window frame
     * @param frameEnd end of the window frame
     */
    @SuppressFBWarnings(value = "SF_SWITCH_FALLTHROUGH",justification = "Intentional")
    public FrameDefinition(int frameMode, int frameStart, long frameStartRows, int frameEnd, long frameEndRows) {
        // default access to allow testing
        this.frameMode = FrameMode.values()[frameMode];
        switch (Frame.values()[frameStart]) {
            case  UNBOUNDED_PRECEDING:
                frameStartRows = Long.MIN_VALUE;
                break;
            case PRECEDING:
                frameStartRows = -1 * frameStartRows;
                break;
            case CURRENT_ROW:
                frameStartRows = 0;
                break;
            case FOLLOWING:
                break;
            case UNBOUNDED_FOLLOWING:
                // This is an error
            default:
                break;
        }
        this.frameStart = new FrameType(frameStart, frameStartRows);


        switch (Frame.values()[frameEnd]) {
            case  UNBOUNDED_FOLLOWING:
                frameEndRows = Long.MAX_VALUE;
                break;
            case PRECEDING:
                frameEndRows = -1 * frameEndRows;
                break;
            case CURRENT_ROW:
                frameEndRows = 0;
            case FOLLOWING:
                break;
            case UNBOUNDED_PRECEDING:
                // This is an error
            default:
                break;
        }
        this.frameEnd = new FrameType(frameEnd, frameEndRows);
    }

    public FrameMode getFrameMode() {
        return frameMode;
    }

    public FrameType getFrameEnd() {
        return frameEnd;
    }

    public FrameType getFrameStart() {
        return frameStart;
    }

    public static FrameDefinition create(FormatableHashtable data) {
        return new FrameDefinition((Integer)data.get("MODE"),
                               (Integer)data.get("START_FRAME"),
                               (Integer)data.get("START_FRAME_ROWS"),
                               (Integer)data.get("END_FRAME"),
                               (Integer)data.get("END_FRAME_ROWS"));
    }

    @Override
    public String toString() {
        return frameMode +
            " BETWEEN " + frameStart +
            " AND " + frameEnd;
    }
}
