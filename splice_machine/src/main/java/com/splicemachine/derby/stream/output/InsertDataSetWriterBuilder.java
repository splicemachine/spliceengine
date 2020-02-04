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

    InsertDataSetWriterBuilder execRowTypeFormatIds(int[] typeFormatIds);

    InsertDataSetWriterBuilder sequences(SpliceSequence[] sequences);

    InsertDataSetWriterBuilder isUpsert(boolean isUpsert);

    InsertDataSetWriterBuilder pkCols(int[] keyCols);

    InsertDataSetWriterBuilder sampleFraction(double sampleFraction);
}
