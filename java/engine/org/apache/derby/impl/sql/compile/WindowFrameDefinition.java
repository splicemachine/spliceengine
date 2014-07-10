package org.apache.derby.impl.sql.compile;

import java.io.Serializable;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * A window function frame definition. Created and validated on the derby side,
 * transported as raw data to splice side.
 *
 * @author Jeff Cunningham
 *         Date: 6/6/14
 */
public class WindowFrameDefinition extends QueryTreeNode implements Serializable {

    public enum FrameMode { ROWS, RANGE }

    public enum Frame {
        UNBOUNDED_PRECEDING,
        PRECEDING,
        CURRENT_ROW,
        FOLLOWING,
        UNBOUNDED_FOLLOWING
    }

    public static class FrameType {
        public static final long NON_VAL = -1;
        private final Frame frame;
        private final long value;

        /**
         *
         * @param frame specifies the set of rows constituting the window frame, for those window functions that
         *              act on the frame instead of the whole partition.
         * @param value The &lt;n&gt; PRECEDING and &lt;n&gt; FOLLOWING cases are currently only allowed in ROWS mode.
         *              They indicate that the frame starts or ends with the row that many rows before or after
         *              the current row. <code>value</code> must be an long and not contain any variables, aggregate
         *              functions, or window functions. The value must not be null or negative; but it can be zero,
         *              which selects the current row itself.<br/>
         *              If <code>frame</code> is not either {@link org.apache.derby.impl.sql.compile.WindowFrameDefinition.Frame#PRECEDING} or
         *              {@link org.apache.derby.impl.sql.compile.WindowFrameDefinition.Frame#FOLLOWING}, <code>value</code> is ignored.
         * @throws StandardException if value is negative when {@link org.apache.derby.impl.sql.compile.WindowFrameDefinition.Frame#PRECEDING} or
         *              {@link org.apache.derby.impl.sql.compile.WindowFrameDefinition.Frame#FOLLOWING} is specified.
         */
        public FrameType(Frame frame, long value) throws StandardException {
            this.frame = frame;
            this.value = value;
            if ( (this.frame.equals(Frame.PRECEDING) || this.frame.equals(Frame.FOLLOWING)) && !( this.value >= 0) ) {
                throw StandardException.newException(SQLState.ID_PARSE_ERROR,
                                     "When window frame is PRECEDING or FOLLOWING, value must be non negative.");
            }
        }

        private FrameType() {
            frame = Frame.UNBOUNDED_PRECEDING;
            value = NON_VAL;
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
            result = (int) (31 * result + value);
            return result;
        }
    }

    private final FrameMode frameMode;
    private final FrameType frameStart;
    private final FrameType frameEnd;

    /**
     * Default ctor: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
     */
    public WindowFrameDefinition() {
        this.frameMode = FrameMode.RANGE;
        this.frameStart = new FrameType();
        this.frameEnd = new FrameType();
    }

    /**
     * Construct a window frame definition
     * @param frameMode frame mode [RANGE | ROWS]
     * @param frameStart start of the window frame
     * @param frameEnd end of the window frame
     * @throws StandardException if <code>frameEnd</code> is defined preceding <code>frameStart</code> or
     * if &lt;n&gt; PRECEDING or &lt;n&gt; FOLLOWING is used in other than ROWS mode.
     */
    public WindowFrameDefinition(FrameMode frameMode, FrameType frameStart, FrameType frameEnd) throws StandardException {
        this.frameMode = frameMode;
        this.frameStart = frameStart;
        this.frameEnd = frameEnd;

        if (this.frameEnd.frame.ordinal() < this.frameStart.frame.ordinal()) {
            throw StandardException.newException(SQLState.ID_PARSE_ERROR,
                                                 "Window frame end cannot precede frame start.");
        }

        if (this.frameMode.equals(FrameMode.RANGE) &&
            (this.frameStart.value >= 0 || this.frameEnd.value >=0)) {
            throw StandardException.newException(SQLState.ID_PARSE_ERROR,
                                                 "Window frame <n> PRECEDING of <n> FOLLOWING is only valid in ROWS mode.");
        }
    }

    /**
     * Provided for planner to use just on referent to a window function. Essentially the
     * same as <code>equals()</code>
     *
     * @param other another frame definition to compare with this one.
     * @return <code>true</code> iff both this instance and <code>other</code> contain
     * the same data.
     */
    public boolean isEquivalent(WindowFrameDefinition other) {
        return this == other || other != null &&
            !(frameMode != null ? !frameMode.equals(other.frameMode) : other.frameMode != null) &&
            !(frameStart != null ? !frameStart.equals(other.frameStart) : other.frameStart != null) &&
            !(frameEnd != null ? !frameEnd.equals(other.frameEnd) : other.frameEnd != null);
    }

    /**
     * Wire transfer of this instance's data. It's already been validated.
     *
     * @return date of this object
     */
    public FormatableHashtable toMap() {
        FormatableHashtable container = new FormatableHashtable();
        container.put("MODE", this.frameMode.ordinal());
        container.put("START_FRAME", this.frameStart.frame.ordinal());
        container.put("START_FRAME_ROWS", this.frameStart.value);
        container.put("END_FRAME", this.frameEnd.frame.ordinal());
        container.put("END_FRAME_ROWS", this.frameEnd.value);
        return container;
    }
}

