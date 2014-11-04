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
 *         Created on: 3/15/13
 */
public class RandomVarcharGenerator implements ColumnDataGenerator{
    private final int numChars;
    private final Random random = new Random();

    public RandomVarcharGenerator(int numChars) {
        this.numChars = numChars;
    }

    @Override
    public void setInto(PreparedStatement ps, int position) throws SQLException {
        ps.setString(position,nextRandomString());
    }

    @Override
    public String toString() {
        return "RandomVarcharGenerator{" +
                "numChars=" + numChars +
                '}';
    }

    public static TypeAdapter<? extends ColumnDataGenerator> getAdapter(){
        return new Adapter();
    }

    private String nextRandomString(){
        int size;
        //loop until we get a non-zero size
        while((size = random.nextInt(numChars))==0){  }

        char[] string = new char[size];
        for(int pos=0;pos<string.length;pos++){
            string[pos] = (char)random.nextInt(255);
        }
        return new String(string);
    }

    private static class Adapter extends TypeAdapter<RandomVarcharGenerator> {
        @Override public void write(JsonWriter out, RandomVarcharGenerator value) throws IOException { }

        @Override
        public RandomVarcharGenerator read(JsonReader in) throws IOException {
            if(in.peek()== JsonToken.NULL){
                in.nextNull();
                return null;
            }
            in.beginObject();
            in.nextName();
            int numChars = in.nextInt();
            in.endObject();
            return new RandomVarcharGenerator(numChars);
        }
    }
}
