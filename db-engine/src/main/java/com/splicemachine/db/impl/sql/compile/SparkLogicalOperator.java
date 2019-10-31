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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;


import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.types.DataType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Function;

import static com.splicemachine.db.impl.sql.compile.BinaryOperatorNode.AND;
import static com.splicemachine.db.impl.sql.compile.BinaryOperatorNode.OR;
import static com.splicemachine.db.impl.sql.compile.RelationalOperator.EQUALS_RELOP;
import static com.splicemachine.db.impl.sql.compile.RelationalOperator.ILLEGAL_UNDEFINED_RELOP;
import static java.lang.String.format;
import static org.apache.spark.sql.functions.lit;

public class SparkLogicalOperator extends AbstractSparkExpressionNode
{
    // See the RelationalOperator interface for valid
    // values of relOpKind
    private int logicalOpKind;

    public SparkLogicalOperator()
    {
    }

    public SparkLogicalOperator(int logicalOpKind)
    {
        this.logicalOpKind = logicalOpKind;
    }

    public static SparkExpressionNode
    getNewSparkLogicalOperator(int logicalOpKind,
                               SparkExpressionNode leftChild,
                               SparkExpressionNode rightChild) {
        if (logicalOpKind == AND) {
            if (leftChild.isTrue())
                return rightChild;
            if (rightChild.isTrue())
                return leftChild;
            if (leftChild.isFalse())
                return leftChild;
            if (rightChild.isFalse())
                return rightChild;
        }
        else if (logicalOpKind == OR) {
            if (leftChild.isTrue())
                return leftChild;
            if (rightChild.isTrue())
                return rightChild;
            if (leftChild.isFalse())
                return rightChild;
            if (rightChild.isFalse())
                return leftChild;
        }
        return new SparkLogicalOperator(logicalOpKind, leftChild, rightChild);
    }

    private SparkLogicalOperator(int logicalOpKind,
                                 SparkExpressionNode leftChild,
                                 SparkExpressionNode rightChild)
    {
        this.logicalOpKind = logicalOpKind;
        this.setLeftChild(leftChild);
        this.setRightChild(rightChild);
    }

    @Override
    public Column getColumnExpression(Dataset<Row> leftDF,
                                      Dataset<Row> rightDF,
                                      Function<String, DataType> convertStringToDataTypeFunction) throws UnsupportedOperationException {
        Column leftExpr  = getLeftChild().getColumnExpression(leftDF, rightDF, convertStringToDataTypeFunction);
        Column rightExpr = getRightChild().getColumnExpression(leftDF, rightDF, convertStringToDataTypeFunction);

        if (logicalOpKind == AND)
            return leftExpr.and(rightExpr);
        else if (logicalOpKind == OR)
            return leftExpr.or(rightExpr);
        else
            throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(getLeftChild().toString());
        if (logicalOpKind == AND)
            sb.append(" and ");
        else if (logicalOpKind == OR)
            sb.append(" or ");
        else
            sb.append(" unknown_op ");
        sb.append(getRightChild().toString());
        sb.append(")");
        return sb.toString();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(serializationVersion);
        if (logicalOpKind != AND && logicalOpKind != OR)
            throw new IOException();
        out.writeInt(logicalOpKind);
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        serializationVersion = in.readInt();
        logicalOpKind = in.readInt();
        if (logicalOpKind != AND && logicalOpKind != OR)
            throw new IOException();
        super.readExternal(in);
    }
}

