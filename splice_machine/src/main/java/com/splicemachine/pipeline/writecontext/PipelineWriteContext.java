package com.splicemachine.pipeline.writecontext;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.google.common.collect.Maps;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.pipeline.writehandler.IndexCallBufferFactory;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class PipelineWriteContext implements WriteContext, Comparable<PipelineWriteContext> {

    private static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
    private static final Logger LOG = Logger.getLogger(PipelineWriteContext.class);
    private static final AtomicInteger idGen = new AtomicInteger(0);

    private final Map<KVPair, WriteResult> resultsMap;
    private final TransactionalRegion rce;
    private final Map<byte[], HTableInterface> tableCache = Maps.newHashMapWithExpectedSize(0);
    private final TxnView txn;
    private final IndexCallBufferFactory indexSharedCallBuffer;
    private final int id = idGen.incrementAndGet();
    private final RegionCoprocessorEnvironment env;
    private final WriteNode head;

    private WriteNode tail;

    public PipelineWriteContext(IndexCallBufferFactory indexSharedCallBuffer, TxnView txn, TransactionalRegion rce, RegionCoprocessorEnvironment env) {
        this(indexSharedCallBuffer, txn, rce, env, true);
    }

    private PipelineWriteContext(IndexCallBufferFactory indexSharedCallBuffer, TxnView txn, TransactionalRegion rce, RegionCoprocessorEnvironment env, boolean keepState) {
        this.indexSharedCallBuffer = indexSharedCallBuffer;
        this.env = env;
        this.rce = rce;
        this.resultsMap = Maps.newIdentityHashMap();
        this.txn = txn;
        this.head = this.tail = new WriteNode(null, this);
    }

    public void addLast(WriteHandler handler) {
        SpliceLogUtils.debug(LOG, "addLast %s", handler);
        WriteNode newWriteNode = new WriteNode(handler, this);
        tail.setNext(newWriteNode);
        tail = newWriteNode;
    }

    @Override
    public void notRun(KVPair mutation) {
        resultsMap.put(mutation, WriteResult.notRun());
    }

    @Override
    public void sendUpstream(KVPair mutation) {
        head.sendUpstream(mutation);
    }

    @Override
    public void failed(KVPair put, WriteResult mutationResult) {
        resultsMap.put(put, mutationResult);
    }

    @Override
    public void success(KVPair put) {
        resultsMap.put(put, WriteResult.success());
    }

    @Override
    public void result(KVPair put, WriteResult result) {
        resultsMap.put(put, result);
    }

    @Override
    public void result(byte[] resultRowKey, WriteResult result) {
        for (KVPair kvPair : resultsMap.keySet()) {
            byte[] currentRowKey = kvPair.getRowKey();
            if (Arrays.equals(currentRowKey, resultRowKey)) {
                resultsMap.put(kvPair, result);
                return;
            }
        }
        throw new IllegalArgumentException("expected existing value in resultsMap");
    }

    @Override
    public HRegion getRegion() {
        return getCoprocessorEnvironment().getRegion();
    }

    @Override
    public HTableInterface getHTable(byte[] indexConglomBytes) {
        HTableInterface table = tableCache.get(indexConglomBytes);
        if (table == null) {
            try {
                table = derbyFactory.getTable(getCoprocessorEnvironment(), indexConglomBytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            tableCache.put(indexConglomBytes, table);
        }
        return table;
    }

    @Override
    public CallBuffer<KVPair> getSharedWriteBuffer(byte[] conglomBytes,
                                                   ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap,
                                                   int maxSize, boolean useAsyncWriteBuffers, TxnView txn) throws Exception {
        assert indexSharedCallBuffer != null;
        return indexSharedCallBuffer.getWriteBuffer(conglomBytes, this, indexToMainMutationMap, maxSize, useAsyncWriteBuffers, txn);
    }

    @Override
    public void flush() throws IOException {
        if (env != null && env.getRegion() != null)
            derbyFactory.checkCallerDisconnect(env.getRegion());

        try {
            WriteNode next = head.getNext();
            while (next != null) {
                next.flush();
                next = next.getNext();
            }
            next = head.getNext();
            while (next != null) {
                next.close();
                next = next.getNext();
            }

        } finally {
            //clean up any outstanding table resources
            for (HTableInterface table : tableCache.values()) {
                try {
                    table.close();
                } catch (Exception e) {
                    //don't need to interrupt the finishing of this batch just because
                    //we got an error. Log it and move on
                    LOG.warn("Unable to clone table", e);
                }
            }
        }
    }

    @Override
    public boolean canRun(KVPair input) {
        WriteResult result = resultsMap.get(input);
        return result == null || result.getCode() == Code.SUCCESS;
    }

    @Override
    public TxnView getTxn() {
        return txn;
    }

    @Override
    public Map<KVPair, WriteResult> close() throws IOException {
        return resultsMap;
    }

    @Override
    public Map<KVPair, WriteResult> currentResults(){
        return resultsMap;
    }

    @Override
    public String toString() {
        return "PipelineWriteContext { region=" + rce.getRegionName() + " }";
    }

    @Override
    public int compareTo(PipelineWriteContext writeContext) {
        return this.id - writeContext.id;
    }

    @Override
    public RegionCoprocessorEnvironment getCoprocessorEnvironment() {
        return env;
    }

    public TransactionalRegion getTransactionalRegion() {
        return rce;
    }
}