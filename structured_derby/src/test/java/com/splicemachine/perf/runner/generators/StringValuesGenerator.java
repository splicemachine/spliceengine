package com.splicemachine.perf.runner.generators;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Created on: 3/17/13
 */
public class StringValuesGenerator implements ColumnDataGenerator {
    private final String[] values;
    private final AtomicInteger currentPosition = new AtomicInteger(0);

    public StringValuesGenerator(String[] values) {
        this.values = values;
    }

    @Override
    public void setInto(PreparedStatement ps, int position) throws SQLException {
        ps.setString(position,values[currentPosition.getAndIncrement()%values.length]);
    }

    public static TypeAdapter<? extends ColumnDataGenerator> getAdapter() {
        return new Adapter();
    }

    private static class Adapter extends TypeAdapter<StringValuesGenerator>{

        @Override public void write(JsonWriter out, StringValuesGenerator value) throws IOException { }

        @Override
        public StringValuesGenerator read(JsonReader in) throws IOException {
            if(in.peek()== JsonToken.NULL){
                in.nextNull();
                return null;
            }

            List<String> vals = new ArrayList<String>();
            in.beginObject();
            while(in.hasNext()){
                String name = in.nextName();
                if("values".equalsIgnoreCase(name)){
                    in.beginArray();
                    while(in.peek()!=JsonToken.END_ARRAY){
                        vals.add(in.nextString());
                    }
                    in.endArray();
                }
            }
            in.endObject();
            return new StringValuesGenerator(vals.toArray(new String[vals.size()]));
        }
    }
}
