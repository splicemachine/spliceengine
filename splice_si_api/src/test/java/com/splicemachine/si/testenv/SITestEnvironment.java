package com.splicemachine.si.testenv;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class SITestEnvironment{
    private static volatile SITestEnv SITestEnv;

    public static SITestEnv loadTestEnvironment(){
        if(SITestEnv==null)
            initialize();

        return SITestEnv;
    }

    private static void initialize(){
        synchronized(SITestEnvironment.class){
            ServiceLoader<SITestEnv> load=ServiceLoader.load(SITestEnv.class);
            Iterator<SITestEnv> iter=load.iterator();
            if(!iter.hasNext())
                throw new IllegalStateException("No SITestEnv found!");
            SITestEnv=iter.next();
            if(iter.hasNext())
                throw new IllegalStateException("Only one SITestEnv is allowed!");
        }
    }

}
