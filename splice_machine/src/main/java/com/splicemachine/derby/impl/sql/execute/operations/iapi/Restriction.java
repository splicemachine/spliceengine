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

package com.splicemachine.derby.impl.sql.execute.operations.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

/**
 * Abstract representation of a Restriction.
 *
 * @author Scott Fines
 * Created on: 10/29/13
 */
public interface Restriction {
    /**
     * Apply a restriction to the merged row.
     *
     * @param row the row to restrict
     * @return true if the row is to be emitted, false otherwise
     * @throws com.splicemachine.db.iapi.error.StandardException if something goes wrong during the restriction
     */
    boolean apply(ExecRow row) throws StandardException;

    static final Restriction noOpRestriction = new Restriction() {
        @Override
        public boolean apply(ExecRow row) throws StandardException {
            return true;
        }
    };
}
