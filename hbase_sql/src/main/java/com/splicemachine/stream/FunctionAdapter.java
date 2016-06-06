package com.splicemachine.stream;

import scala.Function1;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * Created by dgomezferro on 6/1/16.
 */
public class FunctionAdapter extends AbstractFunction1<Iterator<Object>, Object[]> implements Serializable {

    private final static ClassTag<Object> tag = scala.reflect.ClassTag$.MODULE$.apply(Object.class);

    @Override
    public Object[] apply(scala.collection.Iterator<Object> locatedRowIterator) {
        return (Object[]) locatedRowIterator.toArray(tag);
    }
}
