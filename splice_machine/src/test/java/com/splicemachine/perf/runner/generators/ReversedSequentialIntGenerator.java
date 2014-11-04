package com.splicemachine.perf.runner.generators;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Created on: 3/25/13
 */
public class ReversedSequentialIntGenerator extends SequentialIntGenerator{

    public ReversedSequentialIntGenerator(int start) {
        super(start);
    }

    @Override
    public void setInto(PreparedStatement ps, int position) throws SQLException {
        ps.setInt(position,next());
    }

    private int next() {
        return Math.abs(Integer.reverse(value.incrementAndGet()));
    }

    public static TypeAdapter<? extends ColumnDataGenerator> getTypeAdapter() {
        return new Adapter();
    }

    public static class Adapter extends TypeAdapter<ReversedSequentialIntGenerator> {

        @Override public void write(JsonWriter out, ReversedSequentialIntGenerator value) throws IOException { }

        @Override
        public ReversedSequentialIntGenerator read(JsonReader in) throws IOException {
            if(in.peek()== JsonToken.NULL){
                in.nextNull();
                return null;
            }

            in.beginObject();
            String name = in.nextName();
            int start = in.nextInt();
            in.endObject();
            return new ReversedSequentialIntGenerator(start);
        }
    }
}
