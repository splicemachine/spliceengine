package com.splicemachine.access.api;

import java.io.IOException;

/**
 * Created by jleach on 11/18/15.
 */
public interface SSource {
    SConnectionFactory getSpliceConnectionFactory() throws IOException;
    STableFactory getSpliceTableFactory() throws IOException;
    STableInfoFactory getSpliceTableInfoFactory() throws IOException;
}
