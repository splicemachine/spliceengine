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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DateTimeDataValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collections;
import java.util.List;

/**
 * A TimeSpanNode is a node used to represent an added time interval
 * It is not supposed to be bound and should be eliminated during parse time
 * to be replaced by a function call to ADD_{DAYS,MONTHS,YEARS}
 */

@SuppressFBWarnings(value="HE_INHERITS_EQUALS_USE_HASHCODE", justification="DB-9277")
public class TimeSpanNode extends ValueNode
{
    private int unit;
    private ValueNode value;

    public void init(
            Object value,
            Object unit)
            throws StandardException
    {
        this.unit = (int) unit;
        this.value = (ValueNode) value;
        switch (this.unit) {
            case DateTimeDataValue.MONTH_INTERVAL:
            case DateTimeDataValue.DAY_INTERVAL:
            case DateTimeDataValue.YEAR_INTERVAL:
                setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(this.unit));
                break;
            default:
                assert false: "Illegal unit from parser";
        }
    }

    @Override
    protected boolean isEquivalent(ValueNode o) throws StandardException {
        if (isSameNodeType(o))
        {
            TimeSpanNode other = (TimeSpanNode) o;
            return this.unit == other.unit && value.isEquivalent(other.value);
        }
        return false;
    }

    @Override
    public List<? extends QueryTreeNode> getChildren() {
        return Collections.singletonList(value);
    }

    @Override
    public QueryTreeNode getChild(int index) {
        assert index == 0;
        return value;
    }

    @Override
    public void setChild(int index, QueryTreeNode newValue) {
        assert index == 0;
        value = (ValueNode) newValue;
    }

    public ValueNode getValue() {
        return value;
    }

    public int getUnit() {
        return unit;
    }

    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {
        throw StandardException.newException(SQLState.LANG_INVALID_TIME_SPAN_OPERATION);
    }
}
