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

package com.splicemachine.derby.stream.output.update;

import com.splicemachine.si.api.txn.IsolationLevel;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import java.io.IOException;
import com.carrotsearch.hppc.BitSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 *
 *
 *
 */
public class ResultSupplier{
    private Record result;
    private byte[] location;
    private Partition htable;
    private Txn txn;
    private long heapConglom;
    private transient Record dataResult;
    private BitSet interestedFields;

    public ResultSupplier(BitSet interestedFields,Txn txn, long heapConglom) {
        interestedFields = interestedFields;
        this.txn = txn;
        this.heapConglom = heapConglom;
    }

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public void setLocation(byte[] location){
        this.location = location;
        this.result = null;
    }

    public void setResult() throws IOException {
        if(result==null) {
            //need to fetch the latest results
            if(htable==null){
                htable =SIDriver.driver().getTableFactory().getTable(Long.toString(heapConglom));
            }
            dataResult = htable.get(location,txn, IsolationLevel.SNAPSHOT_ISOLATION);
        }
    }

    public void close() throws IOException {
        if(htable!=null)
            htable.close();
    }
}

