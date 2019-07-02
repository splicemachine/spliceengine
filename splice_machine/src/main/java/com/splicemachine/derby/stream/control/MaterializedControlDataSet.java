package com.splicemachine.derby.stream.control;

import com.splicemachine.derby.stream.iapi.DataSet;

import java.util.Collection;
import java.util.Iterator;

/**
 * Created by yxia on 4/17/19.
 */
public class MaterializedControlDataSet<V> extends ControlDataSet<V> {
    Collection<V> dataset;

    public MaterializedControlDataSet(Iterator<V> iterator) {
        super(iterator);
    }

    public MaterializedControlDataSet(Collection<V> dataset) {
        super(dataset.iterator());
        this.dataset = dataset;
    }

    public DataSet getClone() {
        return new MaterializedControlDataSet<V>(dataset);
    }
}
