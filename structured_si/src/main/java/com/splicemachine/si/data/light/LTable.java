package com.splicemachine.si.data.light;

import com.splicemachine.si.data.api.STable;

public class LTable implements STable {
    final String relationIdentifier;

    public LTable(String relationIdentifier) {
        this.relationIdentifier = relationIdentifier;
    }
}
