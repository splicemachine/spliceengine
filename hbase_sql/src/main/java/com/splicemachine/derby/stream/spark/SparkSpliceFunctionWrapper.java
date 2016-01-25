package com.splicemachine.derby.stream.spark;

import com.splicemachine.derby.stream.function.ExternalizableFunction;
import org.apache.spark.api.java.function.Function;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Wrapper function to map between the Spark API and the SpliceMachine
 * tuple maps API.
 *
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkSpliceFunctionWrapper<T,R> implements Function<T,R>,Externalizable{
    private ExternalizableFunction<T,R> delegate;

    public SparkSpliceFunctionWrapper(ExternalizableFunction<T, R> delegate){
        this.delegate=delegate;
    }

    public SparkSpliceFunctionWrapper(){ }

    @Override
    public R call(T t) throws Exception{
        return delegate.call(t);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
       delegate.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        this.delegate = (ExternalizableFunction<T,R>)in.readObject();
    }
}
