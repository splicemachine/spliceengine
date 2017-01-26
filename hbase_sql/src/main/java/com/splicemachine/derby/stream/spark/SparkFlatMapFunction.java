/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.derby.stream.function.AbstractSpliceFunction;
import com.splicemachine.derby.stream.function.ExternalizableFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

/**
 * Mapping between Spark and SpliceMachine functional APIs.
 *
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkFlatMapFunction<T,R> implements FlatMapFunction<T,R>,Externalizable{
    private ExternalizableFlatMapFunction<T,R> delegate;

    public SparkFlatMapFunction(ExternalizableFlatMapFunction<T, R> delegate){
        this.delegate=delegate;
    }

    public SparkFlatMapFunction(){ }

    @Override
    public Iterator<R> call(T t) throws Exception{
        return delegate.call(t);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        out.writeObject(delegate);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        delegate = (ExternalizableFlatMapFunction<T,R>)in.readObject();
    }

    public String getPrettyFunctionName() {
        return ((AbstractSpliceFunction)delegate).getPrettyFunctionName();
    }

}
