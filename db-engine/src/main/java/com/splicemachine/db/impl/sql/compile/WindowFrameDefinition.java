/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import java.io.Serializable;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.shared.common.reference.SQLState;

/**
 * A window function frame definition. Created and validated on the db side,
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
        public static final int NON_VAL = -1;
        private final Frame frame;
        private final int value;

        /**
         *
         * @param frame specifies the set of rows constituting the window frame, for those window functions that
         *              act on the frame instead of the whole partition.
         * @param value The &lt;n&gt; PRECEDING and &lt;n&gt; FOLLOWING cases are currently only allowed in ROWS mode.
         *              They indicate that the frame starts or ends with the row that many rows before or after
         *              the current row. <code>value</code> must be an long and not contain any variables, aggregate
         *              functions, or window functions. The value must not be null or negative; but it can be zero,
         *              which selects the current row itself.<br/>
         *              If <code>frame</code> is not either {@link Frame#PRECEDING} or
         *              {@link Frame#FOLLOWING}, <code>value</code> is ignored.
         * @throws StandardException if value is negative when {@link Frame#PRECEDING} or
         *              {@link Frame#FOLLOWING} is specified.
         */
        public FrameType(Frame frame, int value) throws StandardException {
            if ( (frame.equals(Frame.PRECEDING) || frame.equals(Frame.FOLLOWING)) && !( value >= 0) ) {
                throw StandardException.newException(SQLState.ID_PARSE_ERROR,
                                                     "When window frame is PRECEDING or FOLLOWING, value must be non negative.");
            }
            if (value == 0 &&
                (frame.equals(Frame.PRECEDING) || frame.equals(Frame.FOLLOWING))) {
                // zero offset from rows preceding/following => current row
                this.frame = Frame.CURRENT_ROW;
                this.value = NON_VAL;
            } else {
                this.frame = frame;
                if (frame.equals(Frame.PRECEDING) || frame.equals(Frame.FOLLOWING)) {
                    // a positive value only makes sense for rows preceding/following
                    this.value = value;
                } else {
                    this.value = NON_VAL;
                }
            }
        }

        private FrameType(Frame defaultFrame) {
            frame = defaultFrame;
            value = NON_VAL;
        }

        public int getValue() {
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

        @Override
        public String toString() {
            return ((value != NON_VAL ? value+"" : "") + frame);
        }
    }

    private FrameMode frameMode;
    private final FrameType frameStart;
    private final FrameType frameEnd;

    /**
     * Default ctor: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
     */
    public WindowFrameDefinition(boolean sorted) {
        this.frameMode = FrameMode.RANGE;
        this.frameStart = new FrameType(Frame.UNBOUNDED_PRECEDING);
        if (sorted) {
            this.frameEnd = new FrameType(Frame.CURRENT_ROW);
        }
        else {
            this.frameEnd = new FrameType(Frame.UNBOUNDED_FOLLOWING);
        }
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
        return this == other || other != null && !(frameMode != null ? !frameMode.equals(other.frameMode) :
            other.frameMode != null) && frameStart.equals(other.frameStart) && frameEnd.equals (other.frameEnd);
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

    @Override
    public String toString() {
        return ("frame mode: " + frameMode + " frame start: " + frameStart + " frame end: " + frameEnd);
    }

    @Override
    public String toHTMLString() {
        return ("mode: " + frameMode + " start: " + frameStart + " end: " + frameEnd);
    }

    public FrameMode getFrameMode() {
        return frameMode;
    }

    public void setFrameMode(FrameMode mode) {
        frameMode = mode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WindowFrameDefinition that = (WindowFrameDefinition) o;

        if (frameEnd != null ? !frameEnd.equals(that.frameEnd) : that.frameEnd != null) return false;
        return frameMode == that.frameMode && (frameStart != null ? frameStart.equals(that.frameStart) : that.frameStart == null);

    }

    @Override
    public int hashCode() {
        int result = frameMode != null ? frameMode.hashCode() : 0;
        result = 31 * result + (frameStart != null ? frameStart.hashCode() : 0);
        result = 31 * result + (frameEnd != null ? frameEnd.hashCode() : 0);
        return result;
    }
}

