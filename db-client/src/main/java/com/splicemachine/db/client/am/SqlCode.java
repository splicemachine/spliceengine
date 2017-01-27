/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.am;

// This class is for strong-typing.
//
// Dnc architected codes in the range +/- 4200 to 4299, plus one additional code for -4499.
//
// SQL codes are architected by the product that issues them.
//

public class SqlCode {
    private int code_;

    public SqlCode(int code) {
        code_ = code;
    }

    /**
     * Return the SQL code represented by this instance.
     *
     * @return an SQL code
     */
    public final int getCode() {
        return code_;
    }

    public final static SqlCode invalidCommitOrRollbackUnderXA = new SqlCode(-4200);

    public final static SqlCode invalidSetAutoCommitUnderXA = new SqlCode(-4201);

    public final static SqlCode queuedXAError = new SqlCode(-4203);

    public final static SqlCode disconnectError = new SqlCode(40000);

    public final static SqlCode undefinedError = new SqlCode(-99999);
    
    /** SQL code for SQL state 02000 (end of data). DRDA does not
     * specify the SQL code for this SQL state, but Derby uses 100. */
    public final static SqlCode END_OF_DATA = new SqlCode(100);
}
