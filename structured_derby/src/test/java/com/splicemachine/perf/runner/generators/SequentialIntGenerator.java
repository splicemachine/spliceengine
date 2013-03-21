package com.splicemachine.perf.runner.generators;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Created on: 3/15/13
 */
public class SequentialIntGenerator implements ColumnDataGenerator {
    private final AtomicInteger value;
    private final int startValue;

    public SequentialIntGenerator(int start) {
        this.value = new AtomicInteger(start);
        this.startValue = start;
    }

    @Override
    public void setInto(PreparedStatement ps, int position) throws SQLException {
        ps.setInt(position,value.incrementAndGet());
    }

    @Override
    public String toString() {
        return "SequentialIntGenerator{" +
                "startValue=" + startValue +
                '}';
    }

    public static TypeAdapter<? extends ColumnDataGenerator> getTypeAdapter() {
        return new Adapter();
    }


    public static class Adapter extends TypeAdapter<SequentialIntGenerator> {

        @Override public void write(JsonWriter out, SequentialIntGenerator value) throws IOException { }

        @Override
        public SequentialIntGenerator read(JsonReader in) throws IOException {
            if(in.peek()== JsonToken.NULL){
                in.nextNull();
                return null;
            }

            in.beginObject();
            String name = in.nextName();
            int start = in.nextInt();
            in.endObject();
            return new SequentialIntGenerator(start);
        }
    }

}
