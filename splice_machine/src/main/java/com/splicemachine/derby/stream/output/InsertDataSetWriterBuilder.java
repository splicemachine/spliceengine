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
