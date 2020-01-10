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
