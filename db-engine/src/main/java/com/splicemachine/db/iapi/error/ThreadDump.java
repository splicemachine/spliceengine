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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.error;

/* Until DERBY-289 related issue settle for shared code
 * Engine have similar code as client code even though some of 
 * code is potentially sharable. If you fix a bug in ThreadDump for engine, 
 * please also change the code in 
 * java/shared/com/splicemachine/db/shared/common/sanity/ThreadDump.java for
 * client if necessary.
 */

import java.util.Map;

public class ThreadDump {

    /**
     * 
     * @return A string representation of a full thread dump
     */
    public static String getStackDumpString() {
        StringBuilder sb = new StringBuilder();
        Map<Thread, StackTraceElement[]> st = Thread.getAllStackTraces();
        for (Map.Entry<Thread, StackTraceElement[]> e : st.entrySet()) {
            StackTraceElement[] lines = e.getValue();
            Thread t = e.getKey();
            sb.append("Thread name=").append(t.getName()).append(" id=").append(t.getId()).append(" priority=").append(t.getPriority()).append(" state=").append(t.getState()).append(" isdaemon=").append(t.isDaemon()).append("\n");
            for (StackTraceElement line : lines) {
                sb.append("\t").append(line).append("\n");

            }
            sb.append("\n");
        }
        return sb.toString();
    }

}
