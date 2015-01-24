package org.apache.hadoop.hbase.client;

import com.splicemachine.constants.SpliceConstants;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/15/15
 */
public class MetaScanAction {

    public static void metaScan(MetaScanner.MetaScannerVisitor visitor,TableName tableName) throws IOException {
        MetaScanner.metaScan(SpliceConstants.config,(ClusterConnection)HConnectionManager.getConnection(SpliceConstants.config),visitor,tableName);
    }
}
