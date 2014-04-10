package com.splicemachine.derby.impl.sql.execute.operations;

import java.util.List;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;

import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.derby.iapi.types.DataValueDescriptor;
/**
 * Represents metadata around Scanning operations. One implementation will delegate down to Derby,
 * another to some other method, depending on the shape of the implementation.
 *
 * @author Scott Fines
 * Created on: 10/1/13
 */
interface ScanInformation<T> {

    void initialize(SpliceOperationContext opContext) throws StandardException;

    ExecRow getResultRow() throws StandardException;

    boolean isKeyed() throws StandardException;

    FormatableBitSet getAccessedColumns() throws StandardException;

    FormatableBitSet getAccessedNonPkColumns() throws StandardException;

		/**
		 * Get the key columns which are accessed WITH RESPECT to their position in the key itself.
		 *
		 * @return a bit set representing the key column locations which are interesting to the query.
		 * @throws StandardException
		 */
    FormatableBitSet getAccessedPkColumns() throws StandardException;


    Scan getScan(String txnId) throws StandardException;
    Scan getScan(String txnId, T startKeyHint) throws StandardException;

    Qualifier[][] getScanQualifiers() throws StandardException;

    String printStartPosition(int numOpens) throws StandardException;

    String printStopPosition(int numOpens) throws StandardException;

    long getConglomerateId();
    
    List<Scan> getScans(String txnId, ExecRow startKeyOverride, Activation activation, SpliceOperation top, SpliceRuntimeContext spliceRuntimeContext) throws StandardException;

    int[] getColumnOrdering() throws StandardException;

    SpliceConglomerate getConglomerate() throws StandardException;

		String getTableVersion() throws StandardException;
}
