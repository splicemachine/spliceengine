package com.splicemachine.perf.runner.generators;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Randomly generates integers within a region.
 *
 * @author Scott Fines
 * Created on: 3/17/13
 */
public class RandomUniqueIntGenerator implements ColumnDataGenerator{
    private final Random random = new Random(System.currentTimeMillis());

    private final int start;
    private final int finish;

    private final ConcurrentSkipListSet<Integer> ints = new ConcurrentSkipListSet<Integer>();

    public RandomUniqueIntGenerator(int start, int finish) {
        this.start = start;
        this.finish = finish;
    }

    @Override
    public void setInto(PreparedStatement ps, int position) throws SQLException {
        ps.setInt(position,nextValue());
    }

    private int nextValue() {
        int next = (((finish-start)*random.nextInt(finish))/finish)+start;
        while(!ints.add(next)){
            next = (((finish-start)*random.nextInt(finish))/finish)+start;
        }
        return next;
    }

    public static TypeAdapter<? extends ColumnDataGenerator> getAdapter() {
        return new Adapter();
    }

    private static class Adapter extends TypeAdapter<RandomUniqueIntGenerator>{

        @Override public void write(JsonWriter out, RandomUniqueIntGenerator value) throws IOException { }

        @Override
        public RandomUniqueIntGenerator read(JsonReader in) throws IOException {
            if(in.peek()== JsonToken.NULL){
                in.nextNull();
                return null;
            }

            in.beginObject();
            int start = 0;
            int finish = Integer.MAX_VALUE;
            while(in.hasNext()){
                String name = in.nextName();
                if("start".equalsIgnoreCase(name))
                    start = in.nextInt();
                else if("stop".equalsIgnoreCase(name))
                    finish = in.nextInt();
            }
            in.endObject();

            return new RandomUniqueIntGenerator(start,finish);
        }
    }
}
