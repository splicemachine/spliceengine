package com.splicemachine.derby.test.framework;

import com.google.common.io.Closeables;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.utils.ConglomerateUtils;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Assert;
import org.junit.Ignore;

/**
 * @author Scott Fines
 *         Date: 1/19/16
 */
@Ignore("-sf- DB-4272")
public class SpliceHBaseUtils{
    public static void splitTable(String tableName, String schemaName, int position) throws Exception {
        Scan scan = new Scan();
        scan.setCaching(100);
        scan.addFamily(SIConstants.DEFAULT_FAMILY_BYTES);

        Assert.fail("NOT IMPLEMENTED");
//        long conglomId = getConglomId(tableName, schemaName);
//        HTable table = null;
//        ResultScanner scanner = null;
//        try {
//            table = new HTable(HConfiguration.INSTANCE.unwrapDelegate(), conglomId + "");
//            scanner = table.getScanner(scan);
//            int count = 0;
//            Result result = null;
//            while (count < position) {
//                Result next = scanner.next();
//                if (next == null) {
//                    break;
//                } else {
//                    result = next;
//                }
//                count++;
//            }
//            if (result != null)
//                ConglomerateUtils.splitConglomerate(conglomId,result.getRow());
//        } catch (Exception e) {
//            SpliceLogUtils.logAndThrow(LOG,"error splitting table",e);
//            throw e;
//        } finally {
//            Closeables.closeQuietly(scanner);
//            Closeables.closeQuietly(table);
//        }
    }
}
