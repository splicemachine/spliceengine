package com.splicemachine.perf.runner.generators;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.lang.math.JVMRandom;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

/**
 * @author Scott Fines
 *         Created on: 3/25/13
 */
public class RandomLongGenerator implements ColumnDataGenerator{
    private final long start;
    private final long stop;
    private final Random random = new Random();

    public RandomLongGenerator(long stop, long start) {
        this.stop = stop;
        this.start = start;
    }

    @Override
    public void setInto(PreparedStatement ps, int position) throws SQLException {
        ps.setLong(position,nextValue());
    }

    private long nextValue() {
        long next = random.nextLong()%stop;
        return (((stop-start)*next)/stop) +start;
    }

    public static TypeAdapter<? extends ColumnDataGenerator> getTypeAdapter(){
        return new Adapter();
    }


    private static class Adapter extends TypeAdapter<RandomLongGenerator>{
        @Override public void write(JsonWriter out, RandomLongGenerator value) throws IOException {}

        @Override
        public RandomLongGenerator read(JsonReader in) throws IOException {
            if(in.peek()== JsonToken.NULL){
                in.nextNull();
                return null;
            }
            long start = 0;
            long stop = Long.MAX_VALUE;
            in.beginObject();
            while(in.hasNext()){
                String name = in.nextName();
                if("start".equalsIgnoreCase(name))
                    start = in.nextLong();
                else if("stop".equalsIgnoreCase(name))
                    stop = in.nextLong();
            }
            in.endObject();
            return new RandomLongGenerator(start,stop);
        }
    }
}
