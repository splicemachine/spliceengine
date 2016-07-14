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
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */
package com.splicemachine.dbTesting.system.oe.routines;

import java.math.BigDecimal;
import java.sql.SQLException;

public class Data {
    
    /**
     * Function to provided an updated C_DATA column for a customer account.
     */
    public static String dataForBadCredit(String creditData, int w, int d,
            short cw, short cd, int c, BigDecimal amount) throws SQLException {

        StringBuffer sb = new StringBuffer(600);
        sb.append(" >");
        sb.append(c);
        sb.append(',');
        sb.append(cd);
        sb.append(',');
        sb.append(cw);
        sb.append(',');
        sb.append(d);
        sb.append(',');
        sb.append(w);
        sb.append(',');
        sb.append(amount);
        sb.append(',');
        sb.append("< ");

        sb.append(creditData);
        if (sb.length() > 500)
            sb.setLength(500);

        return sb.toString();
    }

}
