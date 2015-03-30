package com.splicemachine.hbase;

public class HBaseSupportFactory {
    public static String HBASE_SUPPORT_IMPL_CLASS = 
    		"com.splicemachine.hbase.HBaseSupportImpl";
    public static HBaseSupport support;

    static {
        try {
        	support = (HBaseSupport) Class.forName(HBASE_SUPPORT_IMPL_CLASS).newInstance();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
