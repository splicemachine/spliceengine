package com.splicemachine.derby.test.framework;

/**
 * @author Scott Fines
 *         Date: 1/19/16
 */
public class SpliceHBaseUtils{
    public static void splitTable(String tableName, String schemaName, int position) throws Exception {
        Scan scan = new Scan();
        scan.setCaching(100);
        scan.addFamily(SpliceConstants.DEFAULT_FAMILY_BYTES);

        long conglomId = getConglomId(tableName, schemaName);
        HTable table = null;
        ResultScanner scanner = null;
        try {
            table = new HTable(SpliceConstants.config, conglomId + "");
            scanner = table.getScanner(scan);
            int count = 0;
            Result result = null;
            while (count < position) {
                Result next = scanner.next();
                if (next == null) {
                    break;
                } else {
                    result = next;
                }
                count++;
            }
            if (result != null)
                ConglomerateUtils.splitConglomerate(conglomId, result.getRow());
        } catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG, "error splitting table", e);
            throw e;
        } finally {
            Closeables.closeQuietly(scanner);
            Closeables.closeQuietly(table);
        }
    }
}
