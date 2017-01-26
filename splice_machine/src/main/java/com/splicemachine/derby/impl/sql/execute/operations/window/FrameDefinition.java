/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
