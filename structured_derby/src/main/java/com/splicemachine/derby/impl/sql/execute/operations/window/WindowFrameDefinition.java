package com.splicemachine.derby.impl.sql.execute.operations.window;


import java.io.Serializable;

import org.apache.derby.iapi.services.io.FormatableHashtable;

/**
 * @author Jeff Cunningham
 *         Date: 7/10/14
 */
public class WindowFrameDefinition {

    public enum FrameMode { ROWS, RANGE }

    public enum Frame {
        UNBOUNDED_PRECEDING,
        PRECEDING,
        CURRENT_ROW,
        FOLLOWING,
        UNBOUNDED_FOLLOWING
    }

    public static class FrameType implements Serializable {
        public static final long NON_VAL = -1;
        private final Frame frame;
        private final int value;

        private FrameType(int frame, int value) {
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
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FrameType frameType = (FrameType) o;

            return value == frameType.value && frame == frameType.frame;
        }

        @Override
        public int hashCode() {
            int result = frame.hashCode();
            result = 31 * result + value;
            return result;
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
    private WindowFrameDefinition(int frameMode, int frameStart, int frameStartRows,
                                  int frameEnd, int frameEndRows) {
        this.frameMode = FrameMode.values()[frameMode];
        this.frameStart = new FrameType(frameStart, frameStartRows);
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

    public boolean isEquivalent(WindowFrameDefinition other) {
        return this == other || other != null && !(frameMode != null ? !frameMode.equals(other.frameMode) :
            other.frameMode != null) && frameStart.equals(other.frameStart) && frameEnd.equals (other.frameEnd);
    }

    public static WindowFrameDefinition create(FormatableHashtable data) {
        return new WindowFrameDefinition((Integer)data.get("MODE"),
                                         (Integer)data.get("START_FRAME"),
                                         (Integer)data.get("START_FRAME_ROWS"),
                                         (Integer)data.get("END_FRAME"),
                                         (Integer)data.get("END_FRAME_ROWS"));
    }
}
