/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.error.StandardException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.Math;

public class SpliceStddevSamp<K extends Double> extends SpliceUDAVariance<K>
{

    Double result;

    public SpliceStddevSamp() {

    }

    public void init() {
        super.init();
        result = null;
    }

    @Override
    public K terminate() {
       if (result == null) {
           if (count > 1) {
               result = new Double(Math.sqrt(variance/(count-1)));
           } else {
               result = new Double(0);
           }
       }
        return (K) result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(result != null);
        if (result!=null) {
            out.writeDouble(result);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        if (in.readBoolean()) {
            result = in.readDouble();
        }
    }
}

