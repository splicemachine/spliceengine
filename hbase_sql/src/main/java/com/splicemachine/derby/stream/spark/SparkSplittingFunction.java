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
