package org.apache.hadoop.hbase.client;

import com.splicemachine.constants.SpliceConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.MetaScanner;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/15/15
 */
public class MetaScanAction {

    public static void metaScan(MetaScanner.MetaScannerVisitor visitor,HConnection connection,TableName tableName) throws IOException {
        MetaScanner.metaScan(connection,visitor,tableName);
    }

    public static void metaScan(MetaScanner.MetaScannerVisitor visitor,TableName tableName) throws IOException {
        MetaScanner.metaScan(HConnectionManager.getConnection(SpliceConstants.config),visitor,tableName);
    }
}