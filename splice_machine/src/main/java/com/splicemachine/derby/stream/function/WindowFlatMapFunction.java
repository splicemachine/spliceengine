package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Created by jleach on 4/28/15.
 */
public class WindowFlatMapFunction<Op extends SpliceOperation,K,V,U> extends SpliceFlatMapFunction<Op,Iterator<Tuple2<K, V>>, U> {


    @Override
    public Iterable<U> call(Iterator<Tuple2<K, V>> sourceIterator) throws Exception {
        return null;
    }
}
