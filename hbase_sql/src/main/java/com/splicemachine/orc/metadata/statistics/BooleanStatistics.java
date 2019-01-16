/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.splicemachine.orc.metadata.statistics;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.orc.input.SpliceOrcNewInputFormat;

import java.io.IOException;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class BooleanStatistics
{
    // 1 byte to denote if null + 1 byte for the value
    public static final long BOOLEAN_VALUE_BYTES = Byte.BYTES + Byte.BYTES;

    private final long trueValueCount;

    public BooleanStatistics(long trueValueCount)
    {
        this.trueValueCount = trueValueCount;
    }

    public long getTrueValueCount()
    {
        return trueValueCount;
    }
/* msirek-temp->
    public static ColumnStatistics getPartitionColumnStatistics(String value) throws IOException {
        BooleanStatistics booleanStatistics = null;
        if(value != null) {
            try {
                SQLBoolean sqlBoolean = new SQLBoolean();
                sqlBoolean.setValue(value);
                booleanStatistics = new BooleanStatistics(sqlBoolean.getBoolean()? SpliceOrcNewInputFormat.DEFAULT_PARTITION_SIZE:0l);
            } catch (StandardException se) {
                throw new IOException(se);
            }

        }
        return new ColumnStatistics(SpliceOrcNewInputFormat.DEFAULT_PARTITION_SIZE,booleanStatistics,null,null,null,null,null,null);
    }
*/
    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BooleanStatistics that = (BooleanStatistics) o;
        return trueValueCount == that.trueValueCount;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(trueValueCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("trueValueCount", trueValueCount)
                .toString();
    }
}
