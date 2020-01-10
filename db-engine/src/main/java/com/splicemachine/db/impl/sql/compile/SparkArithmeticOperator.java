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
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.types.DataType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Function;


import static com.splicemachine.db.impl.sql.compile.BinaryOperatorNode.*;
import static com.splicemachine.db.impl.sql.compile.RelationalOperator.*;

public class SparkArithmeticOperator extends AbstractSparkExpressionNode
{
    // See the RelationalOperator interface for valid
    // values of relOpKind
    private int opKind;

    public SparkArithmeticOperator()
    {
    }

    public SparkArithmeticOperator(String operatorString,
                                   SparkExpressionNode leftChild,
                                   SparkExpressionNode rightChild)
    {
        if (operatorString.equals("+"))
            this.opKind = PLUS;
        else if (operatorString.equals("-"))
            this.opKind = MINUS;
        else if (operatorString.equals("*"))
            this.opKind = TIMES;
        else if (operatorString.equals("/"))
            this.opKind = DIVIDE;
        else
            this.opKind = -1;
        this.setLeftChild(leftChild);
        this.setRightChild(rightChild);
    }

    @Override
    public Column getColumnExpression(Dataset<Row> leftDF,
                                      Dataset<Row> rightDF,
                                      Function<String, DataType> convertStringToDataTypeFunction) throws UnsupportedOperationException {
        Column leftExpr  = getLeftChild().getColumnExpression(leftDF, rightDF, convertStringToDataTypeFunction);
        Column rightExpr = getRightChild().getColumnExpression(leftDF, rightDF, convertStringToDataTypeFunction);

        if (opKind == PLUS)
            return leftExpr.plus(rightExpr);
        else if (opKind == MINUS)
            return leftExpr.minus(rightExpr);
        else if (opKind == TIMES)
            return leftExpr.multiply(rightExpr);
        else if (opKind == DIVIDE)
            return leftExpr.divide(rightExpr);
        else
            throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(getLeftChild().toString());
        if (opKind == PLUS)
            sb.append(" + ");
        else if (opKind == MINUS)
            sb.append(" - ");
        else if (opKind == TIMES)
            sb.append(" * ");
        else if (opKind == DIVIDE)
            sb.append(" / ");
        else
            sb.append(" unknown_op ");
        sb.append(getRightChild().toString());
        sb.append(")");
        return sb.toString();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(serializationVersion);
        if (opKind < PLUS || opKind > DIVIDE)
            throw new IOException();
        out.writeInt(opKind);
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        serializationVersion = in.readInt();
        opKind = in.readInt();
        if (opKind < PLUS || opKind > DIVIDE)
            throw new IOException();
        super.readExternal(in);
    }
}

