package com.splicemachine.qpt;

public class Configuration {
    enum PrepMode {
        NONE,    // don't prepare
        WHOLE,   // prepare the entire statement as is
        AUTO,    // prepare with auto-parameterization
        SUBST;   // prepare with parameter substitution
    };

    static PrepMode prepare = PrepMode.NONE;
}
