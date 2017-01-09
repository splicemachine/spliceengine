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

package com.splicemachine.pipeline.client;

import com.splicemachine.si.api.txn.Txn;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Extension of BulkWrites to wrap for a region server.
 *
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class BulkWrites{
    private Collection<BulkWrite> bulkWrites;
    private Txn txn;
    /*
     *a key to indicate which region to send the write to. This is really just
     * any region which is present on the destination region server
     */
    private transient byte[] regionKey;

    public BulkWrites(){
        bulkWrites=new ArrayList<>(0);
    }

    public BulkWrites(Collection<BulkWrite> bulkWrites,Txn txn){
        this(bulkWrites,txn,null);
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public BulkWrites(Collection<BulkWrite> bulkWrites,Txn txn,byte[] regionKey){
        this.bulkWrites=bulkWrites;
        this.txn=txn;
        this.regionKey=regionKey;
    }

    @SuppressFBWarnings(value="EI_EXPOSE_REP", justification="Intentional")
    public byte[] getRegionKey(){
        return regionKey;
    }

    public Collection<BulkWrite> getBulkWrites(){
        return bulkWrites;
    }

    public Txn getTxn(){
        return txn;
    }

    public int getBufferHeapSize(){
        int size=0;
        for(BulkWrite bw : bulkWrites){
            size+=bw.getBufferSize();
        }
        return size;
    }

    /**
     * @return the number of rows in the bulk write
     */
    public int numEntries(){
        int size=0;
        for(BulkWrite bw : bulkWrites){
            size+=bw.getSize();
        }
        return size;
    }

    /**
     * @return the number of regions in this write
     */
    public int numRegions(){
        return bulkWrites.size();
    }


    @Override
    public String toString(){
        StringBuilder sb=new StringBuilder();
        sb.append("BulkWrites{");
        boolean first=true;
        for(BulkWrite bw : bulkWrites){
            if(first) first=false;
            else sb.append(",");
            sb.append(bw);
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o){
        if(this==o) return true;
        if(o==null || getClass()!=o.getClass()) return false;

        BulkWrites that=(BulkWrites)o;

        return bulkWrites.equals(that.bulkWrites);

    }

    @Override
    public int hashCode(){
        return bulkWrites.hashCode();
    }

}
