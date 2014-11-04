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
public class IpGenerator implements ColumnDataGenerator{
    private final Random random = new Random();

    @Override
    public void setInto(PreparedStatement ps, int position) throws SQLException {
        ps.setString(position,nextIp());
    }

    private String nextIp() {
        return String.format("%d.%d.%d.%d",
                random.nextInt(256),random.nextInt(256),random.nextInt(256),random.nextInt(256));
    }

    public static TypeAdapter<? extends ColumnDataGenerator> getAdapter() {
        return new Adapter();
    }

    private static class Adapter extends TypeAdapter<IpGenerator>{
        @Override public void write(JsonWriter out, IpGenerator value) throws IOException { }

        @Override
        public IpGenerator read(JsonReader in) throws IOException {
            if(in.peek()== JsonToken.NULL){
                in.nextNull();
                return null;
            }

            in.beginObject();
            in.endObject();
            return new IpGenerator();
        }
    }
}
