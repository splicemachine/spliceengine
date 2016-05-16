package com.splicemachine.derby.stream.spark;

import com.splicemachine.derby.stream.function.ExternalizableFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

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
    public Iterable<R> call(T t) throws Exception{
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
}
