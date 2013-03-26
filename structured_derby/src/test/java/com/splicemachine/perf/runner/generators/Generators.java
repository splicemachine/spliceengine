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
        }else if(RandomIntGenerator.class.getSimpleName().equalsIgnoreCase(typeString)){
            return RandomIntGenerator.getTypeAdapter();
        }else if(SystemTimeGenerator.class.getSimpleName().equalsIgnoreCase(typeString)){
            return SystemTimeGenerator.getAdapter();
        }else if(StringValuesGenerator.class.getSimpleName().equalsIgnoreCase(typeString)){
            return StringValuesGenerator.getAdapter();
        }else if(IpGenerator.class.getSimpleName().equalsIgnoreCase(typeString)){
            return IpGenerator.getAdapter();
        }else if(RandomUniqueIntGenerator.class.getSimpleName().equalsIgnoreCase(typeString)){
            return RandomUniqueIntGenerator.getAdapter();
        }else if(RandomLongGenerator.class.getSimpleName().equalsIgnoreCase(typeString)){
            return RandomLongGenerator.getTypeAdapter();
        }else if(RandomTimestampGenerator.class.getSimpleName().equalsIgnoreCase(typeString)){
            return RandomTimestampGenerator.getTypeAdapter();
        }else if(ReversedSequentialIntGenerator.class.getSimpleName().equalsIgnoreCase(typeString)){
            return ReversedSequentialIntGenerator.getTypeAdapter();
        }

        throw new AssertionError("Unable to determine type adapter for generator type "+ typeString);
    }
}
