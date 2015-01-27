package org.apache.hadoop.hbase.client;

import com.splicemachine.constants.SpliceConstants;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/27/15
 */
public class MetaScanAction {

    public static void metaScan(MetaScanner.MetaScannerVisitor visitor,HConnection connection,TableName tableName) throws IOException {
        MetaScanner.metaScan(SpliceConstants.config,connection,visitor,tableName);
    }

    public static void metaScan(MetaScanner.MetaScannerVisitor visitor,TableName tableName) throws IOException {
        MetaScanner.metaScan(SpliceConstants.config,HConnectionManager.getConnection(SpliceConstants.config),visitor,tableName);
    }
}
