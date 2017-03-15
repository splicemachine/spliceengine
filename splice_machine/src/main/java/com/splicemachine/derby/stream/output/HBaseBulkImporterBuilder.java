package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.api.txn.TxnView;

/**
 * Created by jyuan on 3/14/17.
 */
public interface HBaseBulkImporterBuilder {

    HBaseBulkImporterBuilder tableVersion(String tableVersion);

    HBaseBulkImporterBuilder pkCols(int[] pkCols);

    HBaseBulkImporterBuilder autoIncrementRowLocationArray(RowLocation[] autoIncrementRowLocationArray);

    HBaseBulkImporterBuilder heapConglom(long heapConglom);

    HBaseBulkImporterBuilder execRow(ExecRow execRow);

    HBaseBulkImporterBuilder sequences(SpliceSequence[] spliceSequences);

    HBaseBulkImporterBuilder operationContext(OperationContext operationContext);

    HBaseBulkImporterBuilder txn(TxnView txn);

    HBaseBulkImporter build();
}
