package com.splicemachine.tools;

import com.splicemachine.db.agg.Aggregator;
import com.splicemachine.db.iapi.error.StandardException;

public class StringConcat implements Aggregator<String, String, StringConcat> {
    private StringBuilder agg;

    public StringConcat() {
    }

    @Override
    public void init() {
        agg = new StringBuilder();
    }

    @Override
    public void accumulate(String value) throws StandardException {
        agg.append(value);
    }

    @Override
    public void merge(StringConcat otherAggregator) {
        agg.append(otherAggregator.agg);
    }

    @Override
    public String terminate() {
        return agg.toString();
    }
}