package com.splicemachine.access.api;

import java.io.IOException;

/**
 * Created by jleach on 11/18/15.
 */
public interface SConnectionFactory<Connection,Admin> {
    public Connection getConnection() throws IOException;
    public Admin getAdmin() throws IOException;
}
