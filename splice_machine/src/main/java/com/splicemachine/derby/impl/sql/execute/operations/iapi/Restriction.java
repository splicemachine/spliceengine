/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
