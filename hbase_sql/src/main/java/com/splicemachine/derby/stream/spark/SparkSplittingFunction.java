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

import com.splicemachine.derby.stream.function.SplittingFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Mapping between the Spark and SpliceMachine Function API.
 *
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkSplittingFunction<V,K,U> implements PairFunction<V, K,U>,Externalizable{
    private SplittingFunction<V, K, U> function;

    public SparkSplittingFunction(){
    }

    public SparkSplittingFunction(SplittingFunction<V, K, U> function){
        this.function=function;
    }

    @Override
    public Tuple2<K, U> call(V v) throws Exception{
        return function.call(v);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        out.writeObject(function);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        function = (SplittingFunction<V,K,U>)in.readObject();
    }
}
