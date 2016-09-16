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

package com.splicemachine.customer;

import java.sql.Connection;
import java.sql.DriverManager;

public class NielsenTesting {

    public static String testInternalConnection() throws Exception {
        Connection mconn = DriverManager
                .getConnection("jdbc:default:connection");
        String lv_ref_val = null;
        try {
            if (mconn != null) {
                lv_ref_val = "Got an internal connection";
            }
            else {
                lv_ref_val = "Cannot get an internal connection";
            }
        } finally {
            if (mconn != null) {
                mconn.close();
            }
        }
        return lv_ref_val;
    }
}
