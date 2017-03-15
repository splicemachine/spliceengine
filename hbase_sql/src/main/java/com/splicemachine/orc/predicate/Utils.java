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
package com.splicemachine.orc.predicate;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

public final class Utils
{
    private Utils()
    {
    }

    public static ColumnVector nativeValueToBlock(DataType type, Object object)
    {
        if (object != null && !Primitives.wrap(type.getJavaType()).isInstance(object)) {
            throw new IllegalArgumentException(String.format("Object '%s' does not match type %s", object, type.getJavaType()));
        }
        ColumnVector vector = ColumnVector.allocate(1,type, MemoryMode.ON_HEAP);
        writeNativeValue(type, blockBuilder, object);
        return blockBuilder.build();
    }

    static Object blockToNativeValue(DataType type, ColumnVector columnVector) {
        return readNativeValue(type, block, 0);
    }
}
