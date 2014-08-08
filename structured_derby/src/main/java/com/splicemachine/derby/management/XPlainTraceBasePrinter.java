package com.splicemachine.derby.management;

/**
 * Created by jyuan on 5/15/14.
 */
public abstract class XPlainTraceBasePrinter implements XPlainTracePrinter {

    protected boolean shouldDisplay(int mode, String columnName) {
        boolean display = true;

        if (mode == 0 ) {
            String s = columnName.toUpperCase();
            if (s.compareTo("TOTALWALLTIME") == 0 ||
                s.compareTo("LOCALSCANROWS") == 0 ||
                s.compareTo("LOCALSCANBYTES") == 0 ||
                s.compareTo("ITERATIONS") == 0)
                return true;
            else
                return false;
        }

        return true;
    }
}
