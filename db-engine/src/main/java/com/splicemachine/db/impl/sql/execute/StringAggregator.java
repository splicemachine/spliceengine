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

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.ExecAggregator;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.NumberDataValue;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.db.iapi.types.TypeId;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public final class StringAggregator extends OrderableAggregator {
    private final int VERSION = 1;
    private StringBuilder aggregator;
    private String separator;

    public StringAggregator(){}

    public StringAggregator(String separator) {
        this.separator = separator;
    }

    public Class getAggregatorClass() {
        return aggregator.getClass();
    }

    @Override
    public ExecAggregator setup(ClassFactory cf, String aggregateName, DataTypeDescriptor returnDataType, DataValueDescriptor param) throws StandardException {
        if (aggregator == null) {
            this.aggregator = new StringBuilder();
            separator = param.getString();
        }
        return this;
    }

    protected void accumulate(DataValueDescriptor addend)
            throws StandardException {
        if (aggregator.length() == 0)
            aggregator.append(addend.getString());
        else
            aggregator.append(separator).append(addend.getString());
        return;

    }

    public void merge(ExecAggregator addend)
            throws StandardException {
        if (addend == null) return; //ignore null entries
        StringBuilder other = ((StringAggregator) addend).aggregator;
        if (aggregator.length() == 0)
            aggregator.append(other);
        else
            aggregator.append(separator).append(other);
    }

    public void add(DataValueDescriptor addend) throws StandardException {
        accumulate(addend);
    }

    /**
     * Return the result of the aggregation.  If the count
     * is zero, then we haven't averaged anything yet, so
     * we return null.  Otherwise, return the running
     * average as a double.
     *
     * @return null or the average as Double
     */
    public DataValueDescriptor getResult() throws StandardException {
        if (this.aggregator.length() == 0) {
            return null;
        }
        return new SQLVarchar(aggregator.toString());
    }

    @Override
    public ExecAggregator newAggregator() {
        StringAggregator strAggregator = new StringAggregator(separator);
        strAggregator.aggregator = new StringBuilder();
        return strAggregator;
    }

    /////////////////////////////////////////////////////////////
    //
    // EXTERNALIZABLE INTERFACE
    //
    /////////////////////////////////////////////////////////////

    /**
     * @throws IOException on error
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(VERSION);
        out.writeObject(aggregator);
        out.writeBoolean(eliminatedNulls);
        out.writeUTF(separator);
    }

    /**
     * @throws IOException on error
     * @see java.io.Externalizable#readExternal
     */
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        int v = in.readInt();
        assert v == VERSION;
        aggregator = (StringBuilder) in.readObject();
        eliminatedNulls = in.readBoolean();
        separator = in.readUTF();
    }

    /////////////////////////////////////////////////////////////
    //
    // FORMATABLE INTERFACE
    //
    /////////////////////////////////////////////////////////////

    /**
     * Get the formatID which corresponds to this class.
     *
     * @return the formatID of this class
     */
    public int getTypeFormatId() {
        return StoredFormatIds.AGG_STR_V01_ID;
    }

    public String toString() {
        try {
            return "StringAggregator: { agg=" + (value != null ? value.getString() : "NULL") + "}";
        } catch (StandardException e) {
            return super.toString() + ":" + e.getMessage();
        }
    }
}
