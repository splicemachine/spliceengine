package com.splicemachine.access.iapi;

import java.io.IOException;

/**
 * Created by jleach on 11/18/15.
 */
public interface SpliceConnectionFactory<Connection> {
    public Connection getConnection() throws IOException;
}
