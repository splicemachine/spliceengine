package com.splicemachine.perf.runner.generators;

import com.google.gson.TypeAdapter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author Scott Fines
 *         Created on: 3/17/13
 */
public class RandomUniqueIntGenerator implements ColumnDataGenerator{
    private final Random random = new Random(System.currentTimeMillis());

    private final ConcurrentSkipListSet<Integer> ints = new ConcurrentSkipListSet<Integer>();

    @Override
    public void setInto(PreparedStatement ps, int position) throws SQLException {
        ps.setInt(position,nextValue());
    }

    private int nextValue() {
        int next = random.nextInt();
        while(!ints.add(next)){
            next = random.nextInt();
        }
        return next;
    }

    public static TypeAdapter<? extends ColumnDataGenerator> getAdapter() {
        return new Adapter();
    }

    private static class Adapter extends NoConfigAdapter<RandomUniqueIntGenerator>{

        @Override
        protected RandomUniqueIntGenerator newInstance() {
            return new RandomUniqueIntGenerator();
        }
    }
}
