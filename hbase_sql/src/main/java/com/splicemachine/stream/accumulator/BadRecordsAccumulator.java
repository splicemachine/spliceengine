package com.splicemachine.stream.accumulator;

import org.apache.spark.AccumulableParam;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * Accumulator for Bad Records in a Map.
 *
 * @see org.apache.spark.AccumulableParam
 *
 */
public class BadRecordsAccumulator implements AccumulableParam<List<String>,String> {


    @Override
    public List<String> addAccumulator(List<String> strings, String s) {
        strings.add(s);
        return strings;
    }

    @Override
    public List<String> addInPlace(List<String> strings, List<String> strings2) {
        strings.addAll(strings2);
        return strings;
    }

    @Override
    public List<String> zero(List<String> strings) {
        return new ArrayList<>(strings);
    }
}
