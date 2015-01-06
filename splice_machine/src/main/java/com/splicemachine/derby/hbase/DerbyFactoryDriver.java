package com.splicemachine.derby.hbase;

public class DerbyFactoryDriver {
    public static String DERBY_FACTORY_CLASS = "com.splicemachine.derby.hbase.DerbyFactoryImpl";
    public static DerbyFactory derbyFactory;

    static {
        try {
            derbyFactory = (DerbyFactory) Class.forName(DERBY_FACTORY_CLASS).newInstance();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}