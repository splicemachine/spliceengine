package com.splicemachine.si.impl.iterator;

import org.apache.hadoop.hbase.util.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Reads data from a set of iterators that are expected to provide Data items in order by their ID. Produces the items
 * in overall ID order.
 */
public class OrderedMuxer<ID extends Comparable, Data> implements Iterator<Data> {
    private final List<Pair<Short, Data>> candidates = new ArrayList<Pair<Short, Data>>();
    private final List<? extends Iterator<Data>> sources;
    private final DataIDDecoder<ID, Data> reader;

    public OrderedMuxer(List<? extends Iterator<Data>> sources, DataIDDecoder<ID, Data> reader) {
        this.sources = sources;
        this.reader = reader;
        loadAllCandidates();
    }

    @Override
    public boolean hasNext() {
        return !candidates.isEmpty();
    }

    @Override
    public Data next() {
        Data result = null;
        if (hasNext()) {
            final Pair<Short, Data> selected = findMinimumCandidate();
            candidates.remove(selected);
            loadCandidate(candidateIndex(selected));
            result = candidateData(selected);
        }
        return result;
    }

    @Override
    public void remove() {
        throw new RuntimeException("not implemented");
    }

    private Pair<Short, Data> constructCandidate(short sourceIndex, Data sourceData) {
        return new Pair<Short, Data>(sourceIndex, sourceData);
    }

    private short candidateIndex(Pair<Short, Data> candidate) {
        return candidate.getFirst();
    }

    private Data candidateData(Pair<Short, Data> candidate) {
        return candidate.getSecond();
    }

    private ID candidateKey(Pair<Short, Data> candidate) {
        return reader.getID(candidateData(candidate));
    }

    private void loadAllCandidates() {
        for (short i = 0; i < sources.size(); i++) {
            loadCandidate(i);
        }
    }

    private void loadCandidate(short sourceIndex) {
        final Iterator<Data> source = sources.get(sourceIndex);
        if (source.hasNext()) {
            candidates.add(constructCandidate(sourceIndex, source.next()));
        }
    }

    private Pair<Short, Data> findMinimumCandidate() {
        Pair<Short, Data> minimumCandidate = null;
        for (Pair<Short, Data> candidate : candidates) {
            if (minimumCandidate == null || lessThan(candidate, minimumCandidate)) {
                minimumCandidate = candidate;
            }
        }
        return minimumCandidate;
    }

    private boolean lessThan(Pair<Short, Data> candidate1, Pair<Short, Data> candidate2) {
        return candidateKey(candidate1).compareTo(candidateKey(candidate2)) < 0;
    }

}
