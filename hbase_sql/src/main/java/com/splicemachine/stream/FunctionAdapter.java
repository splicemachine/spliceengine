package com.splicemachine.stream;

import scala.Function1;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * Created by dgomezferro on 6/1/16.
 */
public class FunctionAdapter extends AbstractFunction1<Iterator<String>, String> implements Serializable {

    private final static ClassTag<String> tag = scala.reflect.ClassTag$.MODULE$.apply(String.class);

    @Override
    public String apply(scala.collection.Iterator<String> it) {
        String result = it.next();
        // This iterator is what's returned on ResultStreamer.call(), which is a list of one element
        assert !it.hasNext();
        return result;
    }
}
