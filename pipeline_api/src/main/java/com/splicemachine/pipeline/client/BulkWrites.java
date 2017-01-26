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

package com.splicemachine.pipeline.client;

import com.splicemachine.si.api.txn.TxnView;
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
    private TxnView txn;
    /*
     *a key to indicate which region to send the write to. This is really just
     * any region which is present on the destination region server
     */
    private transient byte[] regionKey;

    public BulkWrites(){
        bulkWrites=new ArrayList<>(0);
    }

    public BulkWrites(Collection<BulkWrite> bulkWrites,TxnView txn){
        this(bulkWrites,txn,null);
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public BulkWrites(Collection<BulkWrite> bulkWrites,TxnView txn,byte[] regionKey){
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

    public TxnView getTxn(){
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
