package com.splicemachine.si2.data.hbase;

import com.splicemachine.si2.data.api.SScan;
import org.apache.hadoop.hbase.client.Scan;

public class HScan implements SScan {
    final Scan scan;

    public HScan(Scan scan) {
        this.scan = scan;
    }

}
