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

import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.catalyst.parser.ParseException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Function;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.lit;

public class SparkConstantExpression extends AbstractSparkExpressionNode
{
    // The string representation of the constant expression.
    private String        constantExpression;
    private String        dataTypeAsString;
    private DataType      dataType = null;
    private SpecialValue  specialValue = SpecialValue.NONE;

    public enum SpecialValue {NONE, NULL, TRUE, FALSE};

    public SparkConstantExpression()
    {
    }

    public SparkConstantExpression(String       constantExpression,
                                   SpecialValue specialValue,
                                   String       dataTypeAsString)
    {
        this.constantExpression = constantExpression;
        this.dataTypeAsString   = dataTypeAsString;
        this.specialValue       = specialValue;
    }

    // Is this a constant TRUE node?
    @Override
    public boolean isTrue() { return specialValue == SpecialValue.TRUE; }

    // Is this a constant FALSE node?
    @Override
    public boolean isFalse() { return specialValue == SpecialValue.FALSE; }

    @Override
    public Column getColumnExpression(Dataset<Row> leftDF,
                                      Dataset<Row> rightDF,
                                      Function<String, DataType> convertStringToDataTypeFunction) throws UnsupportedOperationException {
        if (dataType == null)
            dataType = convertStringToDataTypeFunction.apply(dataTypeAsString);
        if (specialValue == SpecialValue.NULL)
            return lit(null).cast(dataType);
        else
            return lit(constantExpression).cast(dataType);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (specialValue == SpecialValue.TRUE)
            sb.append("true");
        else if (specialValue == SpecialValue.FALSE)
            sb.append("false");
        else {
            sb.append("CAST(");
            if (specialValue == SpecialValue.NULL)
                sb.append("null AS ");
            else
                sb.append(format("%s AS ", constantExpression));
            sb.append(format("%s)", dataTypeAsString));
        }
        return sb.toString();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(serializationVersion);
        out.writeUTF(constantExpression);
        out.writeUTF(dataTypeAsString);
        out.writeUTF(specialValue.name());
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        serializationVersion = in.readInt();
        constantExpression   = in.readUTF();
        dataTypeAsString     = in.readUTF();
        String enumString = in.readUTF();
        if (enumString.equals(SpecialValue.NONE.name()))
            specialValue = SpecialValue.NONE;
        else if (enumString.equals(SpecialValue.NULL.name()))
            specialValue = SpecialValue.NULL;
        else if (enumString.equals(SpecialValue.TRUE.name()))
            specialValue = SpecialValue.TRUE;
        else if (enumString.equals(SpecialValue.FALSE.name()))
            specialValue = SpecialValue.FALSE;
        else
            throw new IOException();
        super.readExternal(in);
    }
}

