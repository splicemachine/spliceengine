package com.splicemachine.perf.runner.qualifiers;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.splicemachine.perf.runner.MapAdapter;

import java.io.IOException;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 3/15/13
 */
public class QualifierAdapter extends TypeAdapter<Qualifier> {
    @Override public void write(JsonWriter out, Qualifier value) throws IOException { }

    @Override
    public Qualifier read(JsonReader in) throws IOException {
        if(in.peek()== JsonToken.NULL){
            in.nextNull();
            return null;
        }

        in.beginObject();
        Map<String,Object> qualifierConfig = null;
        String qualifierType = null;
        while(in.hasNext()){
            String name = in.nextName();
            if("qualifierType".equalsIgnoreCase(name)){
                qualifierType = in.nextString();
            }else if("qualifierConfig".equalsIgnoreCase(name)){
                qualifierConfig = new MapAdapter().read(in);
            }
        }

        in.endObject();

        return Qualifiers.getQualifier(qualifierType,qualifierConfig);
    }
}
