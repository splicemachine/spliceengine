package com.splicemachine.perf.runner.generators;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

/**
 * @author Scott Fines
 *         Created on: 3/17/13
 */
public class RandomIntGenerator implements ColumnDataGenerator{
    private final Random random = new Random();
    private final int start;
    private final int stop;

    public RandomIntGenerator(int start, int stop) {
        this.start = start;
        this.stop = stop;
    }

    @Override
    public void setInto(PreparedStatement ps, int position) throws SQLException {
        ps.setInt(position,nextValue());
    }

    private int nextValue(){
        return (((stop-start)*random.nextInt(stop))/stop)+start;
    }

    public static TypeAdapter<? extends ColumnDataGenerator> getTypeAdapter(){
        return new Adapter();
    }

    private static class Adapter extends TypeAdapter<RandomIntGenerator>{

        @Override public void write(JsonWriter out, RandomIntGenerator value) throws IOException { }

        @Override
        public RandomIntGenerator read(JsonReader in) throws IOException {
            if(in.peek()== JsonToken.NULL){
                in.nextNull();
                return null;
            }
            int start = 0;
            int stop = Integer.MAX_VALUE;
            in.beginObject();
            while(in.hasNext()){
                String name = in.nextName();
                if("start".equalsIgnoreCase(name))
                    start = in.nextInt();
                else if("stop".equalsIgnoreCase(name))
                    stop = in.nextInt();
            }
            in.endObject();
            return new RandomIntGenerator(start,stop);
        }
    }

}
