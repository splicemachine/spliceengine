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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.spark.sql.types.DataType;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

// TODO: When we move RowExpressions to the SPI, we should get rid of this. This is effectively a ConstantExpression.
public final class NullableValue
{
    private final DataType type;
    private final Object value;

    public NullableValue(DataType type, Object value)
    {
        requireNonNull(type, "type is null");
        if (value != null && !Primitives.wrap(type.getJavaType()).isInstance(value)) {
            throw new IllegalArgumentException(String.format("Object '%s' does not match type %s", value, type.getJavaType()));
        }

        this.type = type;
        this.value = value;
    }

    public static NullableValue of(DataType type, Object value)
    {
        requireNonNull(value, "value is null");
        return new NullableValue(type, value);
    }

    public static NullableValue asNull(DataType type)
    {
        return new NullableValue(type, null);
    }

    // Jackson deserialization only
    @JsonCreator
    public static NullableValue fromSerializable(@JsonProperty("serializable") Serializable serializable)
    {
        Type type = serializable.getType();
        Block block = serializable.getBlock();
        return new NullableValue(type, block == null ? null : Utils.blockToNativeValue(type, block));
    }

    // Jackson serialization only
    @JsonProperty
    public Serializable getSerializable()
    {
        return new Serializable(type, value == null ? null : Utils.nativeValueToBlock(type, value));
    }

    public ColumnVector asBlock()
    {
        return Utils.nativeValueToBlock(type, value);
    }

    public DataType getType()
    {
        return type;
    }

    public boolean isNull()
    {
        return value == null;
    }

    public Object getValue()
    {
        return value;
    }

    @Override
    public int hashCode()
    {
        int hash = Objects.hash(type);
        if (value != null) {
            hash = hash * 31 + (int) type.hash(Utils.nativeValueToBlock(type, value), 0);
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NullableValue other = (NullableValue) obj;
        return Objects.equals(this.type, other.type)
                && (this.value == null) == (other.value == null)
                && (this.value == null || type.equalTo(Utils.nativeValueToBlock(type, value), 0, Utils.nativeValueToBlock(other.type, other.value), 0));
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("NullableValue{");
        sb.append("type=").append(type);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    public static class Serializable
    {
        private final DataType type;
        private final ColumnVector block;

        @JsonCreator
        public Serializable(
                @JsonProperty("type") DataType type,
                @JsonProperty("block") ColumnVector block)
        {
            this.type = requireNonNull(type, "type is null");
            this.block = block;
        }

        @JsonProperty
        public DataType getType()
        {
            return type;
        }

        @JsonProperty
        public ColumnVector getBlock()
        {
            return block;
        }
    }
}
