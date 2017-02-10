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

package com.splicemachine.si.impl.region;

import com.carrotsearch.hppc.LongArrayList;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.lifecycle.TxnPartition;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.HCannotCommitException;
import com.splicemachine.si.impl.HReadOnlyModificationException;
import com.splicemachine.si.impl.HTransactionTimeout;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.utils.Source;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Uses an HRegion to access Txn information.
 * <p/>
 * Intended <em>only</em> to be used within a coprocessor on a
 * region of the Transaction table.
 *
 * @author Scott Fines
 *         Date: 6/19/14
 */
public class RegionTxnStore implements TxnPartition{
    private static final Logger LOG=Logger.getLogger(RegionTxnStore.class);

    private final TxnDecoder newTransactionDecoder=V2TxnDecoder.INSTANCE;
    private final TransactionResolver resolver;
    private final TxnSupplier txnSupplier;
    private final HRegion region;
    private final long keepAliveTimeoutMs;
    private final Clock clock;

    public RegionTxnStore(HRegion region,
                          TxnSupplier txnSupplier,
                          TransactionResolver resolver,
                          long keepAliveTimeoutMs,
                          Clock keepAliveClock){
        this.txnSupplier=txnSupplier;
        this.region=region;
        this.resolver=resolver;
        this.keepAliveTimeoutMs = keepAliveTimeoutMs;
        this.clock = keepAliveClock;
    }

    @Override
    public IOException cannotCommit(long txnId,Txn.State state){
        return new HCannotCommitException(txnId,state);
    }

    @Override
    public TxnMessage.Txn getTransaction(long txnId) throws IOException{
        long beginTS = txnId & SIConstants.TRANSANCTION_ID_MASK;
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getTransaction txnId=%d",txnId);
        Get get=new Get(getRowKey(beginTS));
        Result result=region.get(get);
        if(result==null||result.isEmpty())
            return null; //no transaction
        return decode(txnId,result);
    }

    @Override
    public void addDestinationTable(long txnId,byte[] destinationTable) throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"addDestinationTable txnId=%d, desinationTable",txnId,destinationTable);
        Get get=new Get(getRowKey(txnId));
        byte[] destTableQualifier=V2TxnDecoder.DESTINATION_TABLE_QUALIFIER_BYTES;
        get.addColumn(FAMILY,destTableQualifier);
        /*
         * We only need to check the new transaction format, because we will never attempt to elevate
		 * a transaction created using the old transaction format.
		 */

        Result result=region.get(get);
        //should never happen, this is in place to protect against programmer error
        if(result==null||result==Result.EMPTY_RESULT)
            throw new HReadOnlyModificationException("Transaction "+txnId+" is read-only, and was not properly elevated.");
        Cell kv=result.getColumnLatestCell(FAMILY,destTableQualifier);
        byte[] newBytes;
        if(kv==null || kv.getValueLength()<=0){
            newBytes=Encoding.encodeBytesUnsorted(destinationTable);
        }else{
            newBytes=new byte[destinationTable.length+kv.getValueLength()+1];
            System.arraycopy(destinationTable,0,newBytes,0, destinationTable.length);
            System.arraycopy(kv.getValueArray(),kv.getValueOffset(),newBytes, destinationTable.length+1,kv.getValueLength());
        }
        Put put=new Put(get.getRow());
        put.add(FAMILY,destTableQualifier,newBytes);
        region.put(put);
    }

    protected byte[] getRowKey(long txnId){
        return TxnUtils.getRowKey(txnId);
    }

    @Override
    public boolean keepAlive(long txnId) throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"keepAlive txnId=%d",txnId);
        byte[] rowKey=getRowKey(txnId);
        Get get=new Get(rowKey);
        get.addColumn(FAMILY,V2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES);
        get.addColumn(FAMILY,V2TxnDecoder.STATE_QUALIFIER_BYTES);

        //we don't try to keep alive transactions with an old form
        Result result=region.get(get);
        if(result==null) return false; //attempted to keep alive a read-only transaction? a waste, but whatever

        Cell stateKv=result.getColumnLatestCell(FAMILY,V2TxnDecoder.STATE_QUALIFIER_BYTES);
        if(stateKv==null){
            // couldn't find the transaction data, it's fine under Restore Mode, issue a warning nonetheless
            LOG.warn("Couldn't load data for keeping alive transaction "+txnId+". This isn't an issue under Restore Mode");
            return false;
        }
        Txn.State state=Txn.State.decode(stateKv.getValueArray(),stateKv.getValueOffset(),stateKv.getValueLength());
        if(state!=Txn.State.ACTIVE) return false; //skip the put if we don't need to do it
        Cell oldKAKV=result.getColumnLatestCell(FAMILY,V2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES);
        long currTime=clock.currentTimeMillis();
        Txn.State adjustedState=adjustStateForTimeout(state,oldKAKV);
        if(adjustedState!=Txn.State.ACTIVE)
            throw new HTransactionTimeout(txnId);

        Put newPut=new Put(getRowKey(txnId));
        newPut.add(FAMILY,V2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES,Encoding.encode(currTime));
        region.put(newPut); //TODO -sf- does this work when the region is splitting?
        return true;
    }

    @Override
    public Txn.State getState(long txnId) throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getState txnId=%d",txnId);
        byte[] rowKey=getRowKey(txnId);
        Get get=new Get(rowKey);
        get.addColumn(FAMILY,V2TxnDecoder.STATE_QUALIFIER_BYTES);
        get.addColumn(FAMILY,V2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES);
        //add the columns for the new encoding
        Result result=region.get(get);
        if(result==null)
            return null; //indicates that the transaction was probably read only--external callers can figure out what that means

        Cell keepAliveKv;
        Cell stateKv=result.getColumnLatestCell(FAMILY,V2TxnDecoder.STATE_QUALIFIER_BYTES);
        keepAliveKv=result.getColumnLatestCell(FAMILY,V2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES);
        Txn.State state=Txn.State.decode(stateKv.getValueArray(),stateKv.getValueOffset(),stateKv.getValueLength());
        if(state==Txn.State.ACTIVE)
            state=adjustStateForTimeout(state,keepAliveKv);

        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getState returnedState state=%s",state);

        return state;
    }

    @Override
    public void recordTransaction(TxnMessage.TxnInfo txn) throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"recordTransaction txn=%s",txn);
        Put put=newTransactionDecoder.encodeForPut(txn,getRowKey(txn.getTxnId()));
        region.put(put);
    }

    @Override
    public void recordCommit(long txnId,long commitTs) throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"recordCommit txnId=%d, commitTs=%d",txnId,commitTs);
        Put put=new Put(getRowKey(txnId));
        put.add(FAMILY,V2TxnDecoder.COMMIT_QUALIFIER_BYTES,Encoding.encode(commitTs));
        put.add(FAMILY,V2TxnDecoder.STATE_QUALIFIER_BYTES,Txn.State.COMMITTED.encode());
        region.put(put);
    }

    @Override
    public void recordGlobalCommit(long txnId,long globalCommitTs) throws IOException{
        Put put=new Put(getRowKey(txnId));
        put.add(FAMILY,V2TxnDecoder.GLOBAL_COMMIT_QUALIFIER_BYTES,Encoding.encode(globalCommitTs));
        region.put(put);
    }

    @Override
    public long getCommitTimestamp(long txnId) throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getCommitTimestamp txnId=%d",txnId);
        Get get=new Get(getRowKey(txnId));
        get.addColumn(FAMILY,V2TxnDecoder.COMMIT_QUALIFIER_BYTES);
        Result result=region.get(get);
        if(result==null||result==Result.EMPTY_RESULT) return -1l; //no commit timestamp for read-only transactions
        Cell kv;
        if((kv=result.getColumnLatestCell(FAMILY,V2TxnDecoder.COMMIT_QUALIFIER_BYTES))!=null)
            return Encoding.decodeLong(kv.getValueArray(),kv.getValueOffset(),false);
        else{
            throw new IOException("V1 Decoder Required?");
        }
    }

    @Override
    public void recordRollback(long txnId) throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"recordRollback txnId=%d",txnId);
        Put put=new Put(getRowKey(txnId));
        put.add(FAMILY,V2TxnDecoder.STATE_QUALIFIER_BYTES,Txn.State.ROLLEDBACK.encode());
        put.add(FAMILY,V2TxnDecoder.COMMIT_QUALIFIER_BYTES,Encoding.encode(-1));
        put.add(FAMILY,V2TxnDecoder.GLOBAL_COMMIT_QUALIFIER_BYTES,Encoding.encode(-1));
        region.put(put);
    }

    @Override
    public long[] getActiveTxnIds(long afterTs,long beforeTs,byte[] destinationTable) throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getActiveTxnIds beforeTs=%d, afterTs=%s, destinationTable=%s",beforeTs,afterTs,destinationTable);

        Source<TxnMessage.Txn> activeTxn=getActiveTxns(afterTs,beforeTs,destinationTable);
        LongArrayList lal=LongArrayList.newInstance();
        while(activeTxn.hasNext()){
            TxnMessage.Txn next=activeTxn.next();
            TxnMessage.TxnInfo info=next.getInfo();
            lal.add(info.getTxnId());
        }
        return lal.toArray();
    }

    public Source<TxnMessage.Txn> getAllTxns(long minTs,long maxTs) throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getAllTxns minTs=%d, maxTs=%s",minTs,maxTs);
        Scan scan=setupScanOnRange(minTs,maxTs);

        RegionScanner scanner=region.getScanner(scan);

        return new ScanIterator(scanner);
    }

    @Override
    public Source<TxnMessage.Txn> getActiveTxns(long afterTs,long beforeTs,byte[] destinationTable) throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getActiveTxns afterTs=%d, beforeTs=%s",afterTs,beforeTs);
        Scan scan = setupScanOnRange(afterTs, beforeTs);
        scan.setFilter(new ActiveTxnFilter(beforeTs,afterTs,destinationTable,clock,keepAliveTimeoutMs));

        final RegionScanner scanner=region.getScanner(scan);
        return new ScanIterator(scanner){

            @Override
            protected TxnMessage.Txn decode(List<Cell> data) throws IOException{
                TxnMessage.Txn txn = super.decode(data);
                if(txn==null) return null;

                /*
                 * In normal circumstances, we would say that this transaction is active
                 * (since it passed the ActiveTxnFilter).
                 *
                 * However, a child transaction may need to be returned even though
                 * he is committed, because a parent along the chain remains active. In this case,
                 * we need to resolve the effective commit timestamp of the parent, and if that value
                 * is -1, then we return it. Otherwise, just mark the child transaction with a global
                 * commit timestamp and move on.
                 */

                long parentTxnId=txn.getInfo().getParentTxnid();
                if(parentTxnId<0){
                    //we are a top-level transaction
                    return txn;
                }

                switch(txnSupplier.getTransaction(parentTxnId).getEffectiveState()){
                    case ACTIVE:
                        return txn;
                    case ROLLEDBACK:
                        resolver.resolveTimedOut(RegionTxnStore.this,txn);
                        return null;
                    case COMMITTED:
                        resolver.resolveGlobalCommitTimestamp(RegionTxnStore.this,txn);
                        return null;
                }

                return txn;
            }
        };
    }

    public Txn.State adjustStateForTimeout(Txn.State currentState,Cell keepAliveCell){
        return TxnStoreUtils.adjustStateForTimeout(currentState,keepAliveCell,clock,keepAliveTimeoutMs);
    }

    @Override
    public void rollbackTransactionsAfter(long txnId) throws IOException {
        final Source<TxnMessage.Txn> allTxns = getAllTxns(0l, Long.MAX_VALUE);
        final Source<TxnMessage.Txn> uncommittedAfter = new UncommittedAfterSource(allTxns, txnId);
        while (uncommittedAfter.hasNext()) {
            TxnMessage.Txn txn = uncommittedAfter.next();
            recordRollback(txn.getInfo().getTxnId());
        }
        uncommittedAfter.close();
    }

    @Override
    public void recordRollbackSubtransactions(long txnId, long[] subIds) throws IOException {
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"recordRollback txnId=%d",txnId);

        long beginTS = txnId & SIConstants.TRANSANCTION_ID_MASK;

        Put put=new Put(getRowKey(beginTS));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        boolean first = true;
        for (long id : subIds) {
            if (!first) {
                baos.write(0);
            }
            baos.write(Encoding.encode(id));
            first = false;
        }
        put.add(FAMILY,V2TxnDecoder.ROLLBACK_SUBTRANSACTIONS_QUALIFIER_BYTES,baos.toByteArray());
        region.put(put);
    }

    /******************************************************************************************************************/
	/*private helper methods*/

    //easy reference for code clarity
    private static final byte[] FAMILY=SIConstants.DEFAULT_FAMILY_BYTES;


    private Scan setupScanOnRange(long afterTs,long beforeTs){
			  /*
			   * Get the bucket id for the region.
			   *
			   * The way the transaction table is built, a region may have an empty start
			   * OR an empty end, but will never have both
			   */
        byte[] startKey=Bytes.toBytes(afterTs);
        byte[] stopKey = Bytes.toBytes(beforeTs+1);

        byte[] regionKey=region.getRegionInfo().getStartKey();
        byte bucket;
        if(regionKey.length>0)
            bucket = regionKey[0];
        else { // first region, bucket is 0
            bucket = 0;
        }
        byte[] sk = new byte[startKey.length+1];
        sk[0] = bucket;
        System.arraycopy(startKey,0,sk,1,startKey.length);
        startKey = sk;

        byte[] ek = new byte[stopKey.length+1];
        ek[0] = bucket;
        System.arraycopy(stopKey,0,ek,1,stopKey.length);
        stopKey = ek;

        if(Bytes.startComparator.compare(region.getRegionInfo().getStartKey(),startKey)>0)
            startKey=region.getRegionInfo().getStartKey();
        if(Bytes.endComparator.compare(region.getRegionInfo().getEndKey(),stopKey)<0)
            stopKey=region.getRegionInfo().getEndKey();
        Scan scan=new Scan(startKey,stopKey);
        scan.setMaxVersions(1);
        return scan;
    }


    private TxnMessage.Txn decode(long txnId,Result result) throws IOException{
        TxnMessage.Txn txn=newTransactionDecoder.decode(this,txnId,result);
        resolveTxn(txn);
        return txn;

    }

    private TxnMessage.Txn decode(List<Cell> keyValues) throws IOException{
        TxnMessage.Txn txn=newTransactionDecoder.decode(this,keyValues);
        resolveTxn(txn);
        return txn;
    }

    @SuppressFBWarnings(value = "SF_SWITCH_NO_DEFAULT",justification = "Intentional")
    private void resolveTxn(TxnMessage.Txn txn){
        switch(Txn.State.fromInt(txn.getState())){
            case ROLLEDBACK:
                if(isTimedOut(txn)){
                    resolver.resolveTimedOut(RegionTxnStore.this,txn);
                }
                break;
            case COMMITTED:
                if(txn.getInfo().getParentTxnid()>0 && txn.getGlobalCommitTs()<0){
                    /*
                     * Just because the transaction was committed and has a parent doesn't mean that EVERY parent
                     * has been committed; still, submit this to the resolver on the off chance that it
                     * has been fully committed, so we can get away with the global commit work.
                     */
                    resolver.resolveGlobalCommitTimestamp(RegionTxnStore.this,txn);
                }
        }
    }

    private boolean isTimedOut(TxnMessage.Txn txn){
        return (clock.currentTimeMillis()-txn.getLastKeepAliveTime())>keepAliveTimeoutMs;
    }

    private class ScanIterator implements Source<TxnMessage.Txn>{
        private final RegionScanner regionScanner;
        protected TxnMessage.Txn next;
        private List<Cell> currentResults;

        public ScanIterator(RegionScanner scanner){
            this.regionScanner=scanner;
        }

        @Override
        public boolean hasNext() throws IOException{
            if(next!=null) return true;
            if(currentResults==null)
                currentResults=new ArrayList<>(10);
            boolean shouldContinue;
            do{
//                currentResults.clear();
                shouldContinue=regionScanner.next(currentResults);
                if(currentResults.size()<=0) return false;

                this.next = decode(currentResults);
                currentResults.clear();
            }while(next==null && shouldContinue);

            return next!=null;
        }

        protected TxnMessage.Txn decode(List<Cell> data) throws IOException{
            return RegionTxnStore.this.decode(data);
        }

        @Override
        public TxnMessage.Txn next() throws IOException{
            if(!hasNext()) throw new NoSuchElementException();
            TxnMessage.Txn n=next;
            next=null;
            return n;
        }

        @Override
        public void close() throws IOException{
            regionScanner.close();
        }
    }

    private class UncommittedAfterSource implements Source<TxnMessage.Txn> {
        private final Source<TxnMessage.Txn> allTxns;
        private final long afterTs;

        private TxnMessage.Txn current = null;
        public UncommittedAfterSource(Source<TxnMessage.Txn> allTxns,long afterTs){
            this.allTxns=allTxns;
            this.afterTs=afterTs;
        }

        @Override
        public boolean hasNext() throws IOException{
            while(current==null && allTxns.hasNext()){
                TxnMessage.Txn txn = allTxns.next();
                int stateCode = txn.getState();
                Txn.State state=Txn.State.fromInt(stateCode);
                //If a transaction is uncommitted, return it
                switch(state){
                    case ACTIVE:
                        current = txn;
                        return true;
                    case ROLLEDBACK:
                        continue;
                }


                //return a transaciton that is committed after afterTs
                TxnMessage.TxnInfo info=txn.getInfo();
                if(info.getBeginTs()>afterTs||
                        txn.getCommitTs()>afterTs ||
                        txn.getGlobalCommitTs()>afterTs){
                    current = txn;
                }
            }
            return current!=null;
        }

        @Override
        public TxnMessage.Txn next() throws IOException{
            if(!hasNext()) throw new NoSuchElementException();
            TxnMessage.Txn txn = current;
            current = null;
            return txn;
        }

        @Override
        public void close() throws IOException{
            allTxns.close();
        }
    }
}
