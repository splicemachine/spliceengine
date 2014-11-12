package com.splicemachine.perf.runner.generators;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 3/17/13
 */
abstract class NoConfigAdapter<T> extends TypeAdapter<T> {
    @Override public void write(JsonWriter out, T value) throws IOException { }

    @Override
    public T read(JsonReader in) throws IOException {
        if(in.peek()== JsonToken.NULL){
            in.nextNull();
            return null;
        }

        in.beginObject();
        in.endObject();
        return newInstance();
    }

    protected abstract T newInstance();
}
