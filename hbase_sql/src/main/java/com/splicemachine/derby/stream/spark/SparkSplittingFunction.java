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
