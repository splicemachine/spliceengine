package com.splicemachine.perf.runner;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 3/15/13
 */
public class MapAdapter extends TypeAdapter<Map<String,Object>> {
    @Override public void write(JsonWriter out, Map<String, Object> value) throws IOException { }

    @Override
    public Map<String, Object> read(JsonReader in) throws IOException {
        if(in.peek()== JsonToken.NULL){
            in.nextNull();
            return null;
        }

        return (Map<String,Object>)new Gson().getAdapter(Map.class).read(in);

    }
}
