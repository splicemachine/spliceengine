package com.splicemachine.si2.data.helper;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableReader;

import java.util.Iterator;

public class RelationReaderHelper {
    private final SDataLib handler;
    private final STableReader reader;

    public RelationReaderHelper(SDataLib handler, STableReader reader) {
        this.handler = handler;
        this.reader = reader;
    }

    public Object read(STable table, Object tupleKey) {
        SGet get = handler.newGet(tupleKey, null, null, null);
        return reader.get(table, get);
    }

}
