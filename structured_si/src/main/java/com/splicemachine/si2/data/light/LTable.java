package com.splicemachine.si2.data.light;

import com.splicemachine.si2.data.api.STable;

public class LTable implements STable {
    final String relationIdentifier;

    public LTable(String relationIdentifier) {
        this.relationIdentifier = relationIdentifier;
    }
}
