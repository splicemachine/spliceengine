package com.splicemachine.ck;

import com.splicemachine.ck.hwrap.ConnectionWrapper;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class ConnectionSingleton {

    static ConnectionWrapper c;
    public static ConnectionWrapper getConnection(final Configuration config) throws IOException {
        if( c == null ) {
            System.out.print("Trying to connect... ");
            c = new ConnectionWrapper().withConfiguration(config).connect();
            System.out.println("connected!");
        }
        return c;
    }

    public static void close() throws Exception {
        if( c != null ) {
            c.close();
            c = null;
        }
    }
}
