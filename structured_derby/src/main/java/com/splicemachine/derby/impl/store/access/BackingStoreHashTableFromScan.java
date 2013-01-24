/*

   Derby - Class org.apache.derby.impl.store.access.BackingStoreHashTableFromScan

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.derby.impl.store.access;

import org.apache.derby.iapi.error.StandardException; 
import org.apache.derby.iapi.store.access.conglomerate.ScanManager;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowSource;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.log4j.Logger;
import java.util.Enumeration;
import java.util.Properties;

/**

Extend BackingStoreHashtable with the ability to maintain the underlying 
openScan() until the hashtable has been closed.  This is necessary for 
long row access.  Access to long row delays actual objectification until
the columns are accessed, but depends on the underlying table to be still
open when the column is accessed.  

<P>
Transactions are obtained from an AccessFactory.
@see BackingStoreHashtable

**/

class BackingStoreHashTableFromScan extends BackingStoreHashtable {
	private static Logger LOG = Logger.getLogger(BackingStoreHashTableFromScan.class);
    private ScanManager             open_scan;
    public BackingStoreHashTableFromScan(
        TransactionController   tc,
		long                    conglomId,
		int                     open_mode,
        int                     lock_level,
        int                     isolation_level,
		FormatableBitSet                 scanColumnList,
		DataValueDescriptor[]   startKeyValue,
		int                     startSearchOperator,
		Qualifier               qualifier[][],
		DataValueDescriptor[]   stopKeyValue,
		int                     stopSearchOperator,
        long                    max_rowcnt,
        int[]                   key_column_numbers,
        boolean                 remove_duplicates,
        long                    estimated_rowcnt,
        long                    max_inmemory_rowcnt,
        int                     initialCapacity,
        float                   loadFactor,
        boolean                 collect_runtimestats,
		boolean					skipNullKeyColumns,
        boolean                 keepAfterCommit) throws StandardException {
        super(
            tc, 
            (RowSource) null,
            key_column_numbers,
            remove_duplicates,
            estimated_rowcnt,
            max_inmemory_rowcnt,
            initialCapacity,
            loadFactor,
			skipNullKeyColumns,
            keepAfterCommit);
        open_scan =  (ScanManager)
            tc.openScan(
                conglomId,
                false,
                open_mode,
                lock_level,
                isolation_level,
                scanColumnList,
                startKeyValue,
                startSearchOperator,
                qualifier,
                stopKeyValue,
                stopSearchOperator);
  //      SpliceScan sScan = (SpliceScan) open_scan;
   //     sScan.populateMergeSort(key_column_numbers, sScan.getOpenConglom().getFormatIds(),sScan.getOpenConglom().getCollationIds(),"chicken_man");
        
       open_scan.fetchSet(max_rowcnt, key_column_numbers, this);
        if (collect_runtimestats) {
            Properties prop = new Properties();
            open_scan.getScanInfo().getAllScanInfo(prop);
            this.setAuxillaryRuntimeStats(prop);
            prop = null;
        }
       
        
    }

    @Override
    public void close() throws StandardException {
        open_scan.close();
        super.close();
        return;
    }

	@Override
	public Enumeration elements() throws StandardException {
		// TODO Auto-generated method stub
		return super.elements();
	}

	@Override
	public Object get(Object key) throws StandardException {
		// TODO Auto-generated method stub
		return super.get(key);
	}

	@Override
	public void getAllRuntimeStats(Properties prop) throws StandardException {
		// TODO Auto-generated method stub
		super.getAllRuntimeStats(prop);
	}

	@Override
	public Object remove(Object key) throws StandardException {
		// TODO Auto-generated method stub
		return super.remove(key);
	}

	@Override
	public void setAuxillaryRuntimeStats(Properties prop) throws StandardException {
		// TODO Auto-generated method stub
		super.setAuxillaryRuntimeStats(prop);
	}

	@Override
	public boolean putRow(boolean needsToClone, DataValueDescriptor[] row) throws StandardException {
		// TODO Auto-generated method stub
		return super.putRow(needsToClone, row);
	}

	@Override
	public int size() throws StandardException {
		// TODO Auto-generated method stub
		return super.size();
	}
    
}
