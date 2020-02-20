/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations.window.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.WindowFunction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.NumberDataValue;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.iapi.types.SQLVarchar;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class StringAggregator extends SpliceGenericWindowFunction {
    private static final int VERSION = 1;
    private StringBuilder acc;
    private String separator;

    public StringAggregator(){}

    public StringAggregator(String separator) {
        this.separator = separator;
    }

    @Override
    public WindowFunction setup(ClassFactory classFactory, String aggregateName, DataTypeDescriptor returnDataType,
                                FormatableHashtable functionSpecificArgs) throws StandardException {
        super.setup( classFactory, aggregateName, returnDataType );
        acc = new StringBuilder();
        separator = ((DataValueDescriptor) functionSpecificArgs.get("param")).getString();
        return this;
    }

    @Override
    public void accumulate(DataValueDescriptor[] valueDescriptors) throws StandardException {
        this.add(valueDescriptors);
    }

    @Override
    public void reset() {
        super.reset();
        acc = new StringBuilder();
    }

    @Override
    protected void calculateOnAdd(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        if (acc.length() == 0)
            acc.append(dvds[0].getString());
        else
            acc.append(separator).append(dvds[0].getString());
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        String str = dvds[0].getString();
        int from = acc.indexOf(str);
        if (from == -1) return;
        int len = str.length() + separator.length();
        if (len > acc.length()) len = acc.length();
        acc.delete(from, len);
    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        return new SQLVarchar(acc.toString());
    }

    @Override
    public WindowFunction newWindowFunction() {
        return new StringAggregator(separator);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(VERSION);
        out.writeObject(acc);
        out.writeUTF(separator);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int v = in.readInt();
        assert v == VERSION;
        acc = (StringBuilder) in.readObject();
        separator = in.readUTF();
    }

    public String getSeparator() {
        return separator;
    }
}
