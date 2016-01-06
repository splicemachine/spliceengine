package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

/**
 * @author Scott Fines
 *         Date: 1/8/16
 */
public interface UpdateDataSetWriterBuilder extends DataSetWriterBuilder{

    UpdateDataSetWriterBuilder execRowDefinition(ExecRow execRow);

    UpdateDataSetWriterBuilder execRowTypeFormatIds(int[] execRowTypeFormatIds);

    UpdateDataSetWriterBuilder pkCols(int[] pkCols);

    UpdateDataSetWriterBuilder pkColumns(FormatableBitSet pkColumns);

    UpdateDataSetWriterBuilder formatIds(int[] format_ids);

    UpdateDataSetWriterBuilder columnOrdering(int[] colOrdering);

    UpdateDataSetWriterBuilder heapList(FormatableBitSet heapList);

    UpdateDataSetWriterBuilder tableVersion(String tableVersion);
}
