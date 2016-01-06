package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;

/**
 * @author Scott Fines
 *         Date: 1/8/16
 */
public interface InsertDataSetWriterBuilder extends DataSetWriterBuilder{

    InsertDataSetWriterBuilder autoIncrementRowLocationArray(RowLocation[] rowLocations);

    InsertDataSetWriterBuilder execRowDefinition(ExecRow definition);

    InsertDataSetWriterBuilder execRowTypeFormatIds(int[] typeFormatIds);

    InsertDataSetWriterBuilder sequences(SpliceSequence[] sequences);

    InsertDataSetWriterBuilder isUpsert(boolean isUpsert);

    InsertDataSetWriterBuilder pkCols(int[] keyCols);

    InsertDataSetWriterBuilder tableVersion(String tableVersion);
}
