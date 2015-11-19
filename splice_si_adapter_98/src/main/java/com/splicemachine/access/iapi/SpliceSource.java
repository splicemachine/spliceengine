package com.splicemachine.access.iapi;

import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.api.TxnStore;
import java.io.IOException;

/**
 * Created by jleach on 11/18/15.
 */
public interface SpliceSource {
    SpliceConnectionFactory getSpliceConnectionFactory() throws IOException;
    SpliceTableFactory getSpliceTableFactory() throws IOException;
    SpliceTableInfoFactory getSpliceTableInfoFactory() throws IOException;
    TxnStore getTxnStore(TimestampSource ts) throws IOException;
}
