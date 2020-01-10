/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.sql.compile.AccessPath;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;

public class JoinStrategyUtil {

    public static boolean isNonCoveringIndex(Optimizable innerTable) {
        try {
            AccessPath path = innerTable.getCurrentAccessPath();
            if (path != null) {
                ConglomerateDescriptor cd = path.getConglomerateDescriptor();
                if (cd != null && cd.isIndex() && !innerTable.isCoveringIndex(cd)) {
                    return true;
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("could not determine if index is covering", e);
        }
        return false;
    }

}
