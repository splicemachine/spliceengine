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
import com.splicemachine.db.impl.sql.compile.FirstLastValueFunctionDefinition;

/**
 * @author Jeff Cunningham
 *         Date: 9/30/15
 */
public class FirstLastValueFunction extends SpliceGenericWindowFunction {
    boolean isLastValue;
    boolean ignoreNulls;

    @Override
    public WindowFunction setup(ClassFactory cf, String aggregateName, DataTypeDescriptor returnType,
                                FormatableHashtable functionSpecificArgs) {
        super.setup(cf, aggregateName, returnType);
        this.isLastValue = aggregateName.equals("LAST_VALUE");
        this.ignoreNulls = (boolean) functionSpecificArgs.get(FirstLastValueFunctionDefinition.IGNORE_NULLS);
        return this;
    }

    @Override
    public void accumulate(DataValueDescriptor[] valueDescriptors) throws StandardException {
        this.add(valueDescriptors);
    }

    @Override
    protected void calculateOnAdd(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        DataValueDescriptor result = chunk.getResult();
        if (result == null || ignoreNulls && result.isNull()) {
            chunk.setResult(dvds[0].cloneValue(false));
        } else if (isLastValue) {   // We keep setting results if we're calc'ing LAST_VALUE. If FIRST_VALUE, stop on first result.
            DataValueDescriptor input = dvds[0];
            // If we specify ignoreNulls, we don't include them in results. If we're respecting nulls, include them.
            if (! (ignoreNulls && (input == null || input.isNull()))) {
                chunk.setResult(input);
            }
        }
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
       if (isLastValue) {
           // we need to handle the scenario where the last value is the one that is removing, this could happen
           // when ignoreNulls is true
           DataValueDescriptor removedVal = dvds[0];
           if (ignoreNulls && !removedVal.isNull() && chunk.getResult().equals(removedVal)) {
               int position = chunk.last;
               DataValueDescriptor result = null;
               while (position > chunk.first) {
                   result = chunk.get(position-1)[0];
                   if (result !=null && !result.isNull()) {
                       chunk.setResult(result);
                       return;
                   }
                   position --;
               }
               chunk.setResult(result);
           }
       } else {// handle First_Value
           int position = chunk.first;
           DataValueDescriptor result = null;
           while (position < chunk.last) {
               result = chunk.get(position)[0];
               if (result !=null && (!ignoreNulls || !result.isNull())) {
                   chunk.setResult(result);
                   return;
               }
               position ++;
           }
           chunk.setResult(result);
       }
       return;
    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        if (chunks.isEmpty() || chunks.get(0).isEmpty())
            return null;
        int index = (isLastValue ? chunks.size()-1 : 0);
        WindowChunk first = chunks.get(index);
        return first.getResult();
    }

    @Override
    public WindowFunction newWindowFunction() {
        return new FirstLastValueFunction();
    }
}
