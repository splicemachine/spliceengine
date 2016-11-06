/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
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
