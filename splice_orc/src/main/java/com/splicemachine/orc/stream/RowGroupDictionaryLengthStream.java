/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
package com.splicemachine.orc.stream;

import com.splicemachine.orc.checkpoint.LongStreamCheckpoint;
import com.splicemachine.orc.checkpoint.RowGroupDictionaryLengthStreamCheckpoint;

import java.io.IOException;

public class RowGroupDictionaryLengthStream
        extends LongStreamV1
{
    private int entryCount = -1;

    public RowGroupDictionaryLengthStream(OrcInputStream input, boolean signed)
    {
        super(input, signed);
    }

    public int getEntryCount()
    {
        return entryCount;
    }

    @Override
    public Class<RowGroupDictionaryLengthStreamCheckpoint> getCheckpointType()
    {
        return RowGroupDictionaryLengthStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(LongStreamCheckpoint checkpoint)
            throws IOException
    {
        super.seekToCheckpoint(checkpoint);
        RowGroupDictionaryLengthStreamCheckpoint rowGroupDictionaryLengthStreamCheckpoint = (RowGroupDictionaryLengthStreamCheckpoint) checkpoint;
        entryCount = rowGroupDictionaryLengthStreamCheckpoint.getRowGroupDictionarySize();
    }
}
