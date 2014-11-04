package com.splicemachine.perf.runner.generators;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author Scott Fines
 *         Created on: 3/17/13
 */
public class SystemTimeGenerator implements ColumnDataGenerator{

    @Override
    public void setInto(PreparedStatement ps, int position) throws SQLException {
        ps.setTimestamp(position,new Timestamp(System.currentTimeMillis()));
    }

    public static TypeAdapter<? extends ColumnDataGenerator> getAdapter(){
        return new Adapter();
    }

    private static class Adapter extends TypeAdapter<SystemTimeGenerator>{
        @Override public void write(JsonWriter out, SystemTimeGenerator value) throws IOException { }

        @Override
        public SystemTimeGenerator read(JsonReader in) throws IOException {
            if(in.peek()== JsonToken.NULL){
                in.nextNull();
                return null;
            }

            in.beginObject();
            in.endObject();
            return new SystemTimeGenerator();
        }
    }
}
