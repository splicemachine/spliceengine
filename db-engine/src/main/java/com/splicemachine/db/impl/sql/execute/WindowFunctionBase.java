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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.ExecAggregator;
import com.splicemachine.db.iapi.sql.execute.WindowFunction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * Base class for Window Functions that provides a factory interface to create and
 * initialize instances of the splice-side window functions.
 *
 * @author Jeff Cunningham
 *         Date: 8/6/14
 */
public abstract class WindowFunctionBase implements WindowFunction {

    protected ClassFactory classFactory;
    protected String windowFunctionName;
    protected DataTypeDescriptor returnDataType;
    protected FormatableHashtable functionSpecificArgs;

    @Override
    public WindowFunction setup(ClassFactory classFactory,
                                String windowFunctionName,
                                DataTypeDescriptor returnDataType,
                                FormatableHashtable functionSpecificArgs) {
        this.classFactory = classFactory;
        this.windowFunctionName = windowFunctionName;
        this.returnDataType = returnDataType;
        this.functionSpecificArgs = functionSpecificArgs;
        return this;
    }

    public WindowFunction newWindowFunction(String className) {
        WindowFunction windowFunctionInstance;
        try{
            Class windowFunctionClass = classFactory.loadApplicationClass(className);
            Object newInstance = windowFunctionClass.newInstance();
            windowFunctionInstance = (WindowFunction)newInstance;
            // the splice-side instance is invoked here
            windowFunctionInstance = windowFunctionInstance.setup(
                classFactory,
                windowFunctionName,
                returnDataType,
                functionSpecificArgs
            );
        }catch(Exception e){
            throw new RuntimeException(e);
        }
        return windowFunctionInstance;
    }

    // no-op interface implementations for db side implementations


    @Override
    public void accumulate(DataValueDescriptor addend, Object ga) throws StandardException {

    }

    @Override
    public void add(DataValueDescriptor addend) throws StandardException {

    }

    @Override
    public void merge(ExecAggregator inputAggregator) throws StandardException {

    }

    @Override
    public ExecAggregator newAggregator() {
        return null;
    }

    @Override
    public boolean didEliminateNulls() {
        return false;
    }

    @Override
    public void accumulate(DataValueDescriptor[] valueDescriptors) throws StandardException {

    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        return null;
    }

    @Override
    public void reset() {}

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }

    @Override
    public int getTypeFormatId() {
        return 0;
    }

    @Override
    public WindowFunction setup(ClassFactory classFactory, String aggregateName, DataTypeDescriptor returnDataType) {
        return null;
    }

    @Override
    public boolean isUserDefinedAggregator() {
        return false;
    }
}
