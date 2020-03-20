/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;


import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Function;

public abstract class AbstractSparkExpressionNode implements SparkExpressionNode
{
    protected SparkExpressionNode left;
    protected SparkExpressionNode right;

    protected int abstractSerializationVersion = 1;
    protected int serializationVersion = 1;

    @Override
    public void setLeftChild(SparkExpressionNode left) {
        this.left = left;
    }

    @Override
    public void setRightChild(SparkExpressionNode right) {
        this.right = right;
    }

    @Override
    public SparkExpressionNode getLeftChild() { return left; }

    @Override
    public SparkExpressionNode getRightChild() { return right; }

    @Override
    public boolean isTrue() { return false; }

    @Override
    public boolean isFalse() { return false; }

    @Override
    public Column getColumnExpression(Dataset<Row> leftDF,
                                      Dataset<Row> rightDF,
                                      Function<String, DataType> convertStringToDataTypeFunction) throws UnsupportedOperationException{
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(abstractSerializationVersion);
        out.writeObject(left);
        out.writeObject(right);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        abstractSerializationVersion = in.readInt();
        left = (SparkExpressionNode)in.readObject();
        right = (SparkExpressionNode)in.readObject();
    }
}


