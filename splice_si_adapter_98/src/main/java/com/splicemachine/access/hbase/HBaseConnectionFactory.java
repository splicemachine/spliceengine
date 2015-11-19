package com.splicemachine.access.hbase;

import com.splicemachine.access.iapi.SpliceConnectionFactory;
import com.splicemachine.constants.SpliceConstants;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import java.io.IOException;

/**
 * Created by jleach on 11/18/15.
 */
public class HBaseConnectionFactory implements SpliceConnectionFactory<Connection> {
    private static Connection connection;
    private static HBaseConnectionFactory INSTANCE = new HBaseConnectionFactory();

    private HBaseConnectionFactory() {

    }

    public static HBaseConnectionFactory getInstance() {
        return INSTANCE;
    }

    static {
        try {
            connection = ConnectionFactory.createConnection(SpliceConstants.config);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    public Connection getConnection() throws IOException {
        return connection;
    }

    public static Admin getAdmin() throws IOException {
        return connection.getAdmin();
    }

}
