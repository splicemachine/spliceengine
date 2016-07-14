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

package com.splicemachine.derby.stream.spark;

import com.splicemachine.derby.stream.function.ZipperFunction;
import org.apache.spark.api.java.function.Function2;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Mapping between the Spark API and the SpliceMachine functional API
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkSpliceFunctionWrapper2<T1,T2,R> implements Function2<T1,T2,R>,Externalizable{
    private ZipperFunction<T1,T2,R> delegate;

    public SparkSpliceFunctionWrapper2(ZipperFunction<T1, T2, R> delegate){
        this.delegate=delegate;
    }

    public SparkSpliceFunctionWrapper2(){
    }

    @Override
    public R call(T1 t1,T2 t2) throws Exception{
        return delegate.call(t1,t2);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        out.writeObject(delegate);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        delegate = (ZipperFunction<T1,T2,R>)in.readObject();
    }
}
