package com.splicemachine.perf.runner.qualifiers;

import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 3/15/13
 */
public class Qualifiers {

    private Qualifiers(){}

    public static Qualifier getQualifier(String qualifierType,Map<String,Object> qualifierConfig){
        if(IntRange.class.getSimpleName().equalsIgnoreCase(qualifierType)){
            return IntRange.create(qualifierConfig);
        }

        throw new AssertionError("Unknown Qualifier: "+ qualifierType);
    }
}
