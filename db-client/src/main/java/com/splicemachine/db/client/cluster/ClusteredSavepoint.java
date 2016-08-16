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
 *
 */

package com.splicemachine.db.client.cluster;

import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * @author Scott Fines
 *         Date: 9/26/16
 */
class ClusteredSavepoint implements Savepoint{
    private final RefCountedConnection rcc;
    private final Savepoint baseSp;

    ClusteredSavepoint(RefCountedConnection rcc,Savepoint baseSp){
        this.rcc=rcc;
        this.baseSp=baseSp;
    }

    @Override
    public int getSavepointId() throws SQLException{
        return baseSp.getSavepointId();
    }

    @Override
    public String getSavepointName() throws SQLException{
        return baseSp.getSavepointName();
    }

    public void rollback() throws SQLException{
        rcc.element().rollback(baseSp);
        rcc.release();
    }

    public void commit() throws SQLException{
        rcc.element().releaseSavepoint(baseSp);
        rcc.release();
    }
}
