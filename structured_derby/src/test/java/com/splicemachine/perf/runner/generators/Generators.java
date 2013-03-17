package com.splicemachine.perf.runner.generators;

import com.google.gson.TypeAdapter;

/**
 * @author Scott Fines
 *         Created on: 3/15/13
 */
public class Generators {

    private Generators(){}

    public static TypeAdapter<? extends ColumnDataGenerator> getGenerator(String typeString){
        if(SequentialIntGenerator.class.getSimpleName().equalsIgnoreCase(typeString)){
            return SequentialIntGenerator.getTypeAdapter();
        }else if(RandomVarcharGenerator.class.getSimpleName().equalsIgnoreCase(typeString)){
            return RandomVarcharGenerator.getAdapter();
        }

        throw new AssertionError("Unable to determine type adapter for generator type "+ typeString);
    }
}
