package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;

/**
 * @author Jeff Cunningham
 *         Date: 6/6/14
 */
public class WindowFrameDefinition extends QueryTreeNode {

    public enum Window { ROWS, RANGE }

    public enum Frame {
        UNBOUNDED_PRECEDING,
        PRECEDING,
        CURRENT_ROW,
        FOLLOWING,
        UNBOUNDED_FOLLOWING
    }

    public static class FrameType {
        private final Frame frame;
        private final int value;

        /**
         *
         * @param frame specifies the set of rows constituting the window frame, for those window functions that
         *              act on the frame instead of the whole partition.
         * @param value The value PRECEDING and value FOLLOWING cases are currently only allowed in ROWS mode.
         *              They indicate that the frame starts or ends with the row that many rows before or after
         *              the current row. value must be an integer expression not containing any variables, aggregate
         *              functions, or window functions. The value must not be null or negative; but it can be zero,
         *              which selects the current row itself.<br/>
         *              If <code>frame</code> is not either {@link org.apache.derby.impl.sql.compile.WindowFrameDefinition.Frame#PRECEDING} or
         *              {@link org.apache.derby.impl.sql.compile.WindowFrameDefinition.Frame#FOLLOWING}, <code>value</code> is ignored.
         * @throws StandardException if value is negative when {@link org.apache.derby.impl.sql.compile.WindowFrameDefinition.Frame#PRECEDING} or
         *              {@link org.apache.derby.impl.sql.compile.WindowFrameDefinition.Frame#FOLLOWING} is specified.
         */
        public FrameType(Frame frame, int value) {
            this.frame = frame;
            this.value = value;
//            if ( (this.frame.equals(Frame.PRECEDING) || this.frame.equals(Frame.FOLLOWING)) && !( this.value >= 0) ) {
//                throw StandardException.newException(SQLState.ID_PARSE_ERROR,
//                                                 "When window frame is PRECEDING or FOLLOWING, value must be non negative.");
//            }
        }

        public int getValue() {
            return value;
        }

        public Frame getFrame() {
            return frame;
        }
    }

    private final Window window;
    private final FrameType frameStart;
    private final FrameType frameEnd;
    public WindowFrameDefinition(Window window, FrameType frameStart, FrameType frameEnd) {
        this.window = window;
        this.frameStart = frameStart;
        this.frameEnd = frameEnd;
    }

    public Window getWindow() {
        return window;
    }

    public FrameType getFrameEnd() {
        return frameEnd;
    }

    public FrameType getFrameStart() {
        return frameStart;
    }

}

