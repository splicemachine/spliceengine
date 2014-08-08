package com.splicemachine.derby.impl.sql.execute.operations.window;


import java.io.Serializable;

import org.apache.derby.iapi.services.io.FormatableHashtable;

/**
 * @author Jeff Cunningham
 *         Date: 7/10/14
 */
public class WindowFrame {

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

        private FrameType(int frame, long value) {
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
    private WindowFrame(int frameMode, int frameStart, long frameStartRows,
                                  int frameEnd, long frameEndRows) {
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

    public boolean isEquivalent(WindowFrame other) {
        return this == other || other != null && !(frameMode != null ? !frameMode.equals(other.frameMode) :
            other.frameMode != null) && frameStart.equals(other.frameStart) && frameEnd.equals (other.frameEnd);
    }

    public static WindowFrame create(FormatableHashtable data) {
        return new WindowFrame((Integer)data.get("MODE"),
                               (Integer)data.get("START_FRAME"),
                               (Integer)data.get("START_FRAME_ROWS"),
                               (Integer)data.get("END_FRAME"),
                               (Integer)data.get("END_FRAME_ROWS"));
    }
}
