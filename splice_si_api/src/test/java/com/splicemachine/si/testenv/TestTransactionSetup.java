/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.si.testenv;

import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.ManualKeepAliveScheduler;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.KeepAliveScheduler;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.server.SITransactor;
import com.splicemachine.si.impl.store.CompletedTxnCacheSupplier;
import com.splicemachine.si.impl.txn.SITransactionReadController;
import com.splicemachine.si.jmx.ManagedTransactor;
import com.splicemachine.storage.DataFilter;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.Partition;
import com.splicemachine.timestamp.api.TimestampSource;

import java.io.IOException;


/**
 * A Setup class for use in testing code.
 */
@SuppressWarnings("unchecked")
public class TestTransactionSetup {

    byte[] family;
    byte[] ageQualifier;
    int agePosition = 0;
    int jobPosition = 1;

    TxnOperationFactory txnOperationFactory;
    public Transactor transactor;
    public TimestampSource timestampSource;
    public TransactionReadController readController;

    public final TxnStore txnStore;
    public TxnLifecycleManager txnLifecycleManager;
    private DataFilterFactory filterFactory;

    public TestTransactionSetup(SITestEnv testEnv, boolean simple) {

        family = SIConstants.DEFAULT_FAMILY_BYTES;
        ageQualifier = Bytes.toBytes("age");

        final ManagedTransactor listener = new ManagedTransactor();

        timestampSource = testEnv.getTimestampSource();
        ClientTxnLifecycleManager lfManager = new ClientTxnLifecycleManager(timestampSource,testEnv.getExceptionFactory());

        txnStore = testEnv.getTxnStore();
        TxnSupplier txnSupplier=new CompletedTxnCacheSupplier(txnStore,100,16);
        filterFactory = testEnv.getFilterFactory();
        lfManager.setTxnStore(txnStore);
        txnLifecycleManager = lfManager;

        txnOperationFactory = testEnv.getOperationFactory();


        KeepAliveScheduler keepAliveScheduler=new ManualKeepAliveScheduler(txnStore);
        lfManager.setKeepAliveScheduler(keepAliveScheduler);
        ((ClientTxnLifecycleManager) txnLifecycleManager).setKeepAliveScheduler(keepAliveScheduler);

        readController = new SITransactionReadController(txnSupplier);

        transactor = new SITransactor(txnSupplier,
                txnOperationFactory,
                testEnv.getBaseOperationFactory(),
                testEnv.getOperationStatusFactory(),
                testEnv.getExceptionFactory());

        if (!simple) {
            listener.setTransactor(transactor);
        }
    }

    public DataFilter equalsValueFilter(byte[] qualifier,byte[] value){
        return filterFactory.singleColumnEqualsValueFilter(SIConstants.DEFAULT_FAMILY_BYTES,qualifier,value);
    }

    public Partition getPersonTable(SITestEnv testEnv) throws IOException{
        return testEnv.getPersonTable(this);
    }

		/*
         * The following methods are in place to bridge the goofiness gap between real code (i.e. HBase) and
		 * the stupid test code, without requiring odd production-level classes and methods which don't have good
		 * type signatures and don't make sense within the system. Someday, we'll remove the test Operation logic
		 * entirely and replace it with an in-memory HBase installation
		 */

//    public OperationWithAttributes convertTestTypePut(Put put) {
//        if (isInMemory) {
//            OperationWithAttributes owa = new LTuple(put.getRow(), Lists.newArrayList(Iterables.concat(put.getFamilyMap().values())));
//            copyAttributes(put, owa);
//            return owa;
//        } else return put;
//    }
//
//    private static void copyAttributes(OperationWithAttributes source, OperationWithAttributes dest) {
//        Map<String, byte[]> attributesMap = source.getAttributesMap();
//        for (Map.Entry<String, byte[]> attribute : attributesMap.entrySet()) {
//            dest.setAttribute(attribute.getKey(), attribute.getValue());
//        }
//    }
//
//
//    public OperationWithAttributes convertTestTypeGet(Get scan, Long effectiveTimestamp) {
//        if (isInMemory) {
//            List<List<byte[]>> columns = Lists.newArrayList();
//            List<byte[]> families = Lists.newArrayList();
//            Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
//            for (byte[] family : familyMap.keySet()) {
//                families.add(family);
//                List<byte[]> columnsForFamily = Lists.newArrayList(familyMap.get(family));
//                columns.add(columnsForFamily);
//            }
//            if (families.size() <= 0)
//                families = null;
//            if (columns.size() <= 0)
//                columns = null;
//
//            OperationWithAttributes owa = new LGet(scan.getRow(), scan.getRow(),
//                    families,
//                    columns, effectiveTimestamp, scan.getMaxVersions());
//            copyAttributes(scan, owa);
//            return owa;
//        } else return scan;
//
//    }
}
