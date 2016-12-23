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

package com.splicemachine.si.testenv;

import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.*;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.server.SITransactor;
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

    public TxnLifecycleManager txnLifecycleManager;
    private DataFilterFactory filterFactory;
    public TransactionStore txnStore;

    public TestTransactionSetup(SITestEnv testEnv, boolean simple) {

        family = SIConstants.DEFAULT_FAMILY_BYTES;
        ageQualifier = Bytes.toBytes("age");

        final ManagedTransactor listener = new ManagedTransactor();

        timestampSource = testEnv.getTimestampSource();
        ClientTxnLifecycleManager lfManager = new ClientTxnLifecycleManager(timestampSource,
                timestampSource,
                testEnv.getExceptionFactory(),
                testEnv.getTxnFactory(),
                testEnv.getTxnLocationFactory(),
                testEnv.getGlobalTxnCache(),
                testEnv.getTxnStore());
        this.txnStore = testEnv.getTxnStore();
//        TxnSupplier txnSupplier=new GlobalTxnCacheSupplier(100,16);
        filterFactory = testEnv.getFilterFactory();
        txnLifecycleManager = lfManager;

        txnOperationFactory = testEnv.getOperationFactory();

        readController = new SITransactionReadController();

        transactor = new SITransactor(testEnv.getTxnStore(),
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
