/*
 *  Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 *  This file is part of Splice Machine.
 *  Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 *  GNU Affero General Public License as published by the Free Software Foundation, either
 *  version 3, or (at your option) any later version.
 *  Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Affero General Public License for more details.
 *  You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 *  If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.sql.compile.Visitable;

class CorrelationUtil {
    /**
     * @return true if <code>node</code> correlates to another query, otherwise false.
     */
    public static boolean isCorrelated(Visitable node) {
        if (node instanceof ColumnReference) {
            return ((ColumnReference) node).getCorrelated();
        } else if (node instanceof VirtualColumnNode) {
            return ((VirtualColumnNode) node).getCorrelated();
        } else if (node instanceof MethodCallNode) {
            /* trigger action references are correlated */
            return ((MethodCallNode) node).getMethodName().equals("getTriggerExecutionContext") ||
                    ((MethodCallNode) node).getMethodName().equals("TriggerOldTransitionRows") ||
                    ((MethodCallNode) node).getMethodName().equals("TriggerNewTransitionRows");
        }
        return false;
    }
}
