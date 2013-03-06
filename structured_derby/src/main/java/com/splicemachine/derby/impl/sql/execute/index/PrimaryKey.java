package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class PrimaryKey implements Constraint{
    private static final Logger logger = Logger.getLogger(PrimaryKey.class);
    private final TableSource tableSource;
    private final byte[] tableBytes;

    public PrimaryKey(String tableName,TableSource tableSource ) {
        this.tableSource = tableSource;
        this.tableBytes = Bytes.toBytes(tableName);
    }

    @Override
    public boolean validate(Put put) throws IOException {
        SpliceLogUtils.trace(logger,"Validating put");
        //make sure it's not already there
        Get get = new Get(put.getRow());
        get.addFamily(HBaseConstants.DEFAULT_FAMILY_BYTES);
        return !tableSource.getTable(tableBytes).exists(get);
    }

    /**
     * Validate the Primary Key Constraints for this table.
     *
     * @param put
     * @param region
     * @return
     * @throws IOException
     */
    public boolean validate(Put put, HRegion region) throws IOException{
        /*
         * If the put is tagged as an update, we don't validate.
         *
         * This is because Updates either change Primary Keys or they do not. If
         * they change primary keys, they must necessarily delete a row and create a
         * new one, which will mean that this put won't be tagged as an insert, and
         * it can be checked then. If the primary keys do not change, then this
         * validator should allow it through anyway
         */
        if(Arrays.equals(put.getAttribute(Puts.PUT_TYPE),Puts.FOR_UPDATE)) return true;

        SpliceLogUtils.trace(logger,"Validating local put");
        Get get = new Get(put.getRow());
        get.addFamily(HBaseConstants.DEFAULT_FAMILY_BYTES);

        Result result = region.get(get,null);

        boolean rowPresent = result!=null && !result.isEmpty();
        SpliceLogUtils.trace(logger,rowPresent? "row exists!": "row not yet present");
        if(rowPresent)
            SpliceLogUtils.trace(logger,result.toString());
        return !rowPresent;
    }

    @Override
    public boolean validate(Delete delete) throws IOException {
        Get get = new Get(delete.getRow());
        get.addColumn(ForeignKey.FOREIGN_KEY_FAMILY, ForeignKey.FOREIGN_KEY_COLUMN);

        Result result  = tableSource.getTable(tableBytes).get(get);
        if(result==null||result.isEmpty()) return true;

        byte[] value = result.getValue(ForeignKey.FOREIGN_KEY_FAMILY, ForeignKey.FOREIGN_KEY_COLUMN);
        return value == null || Bytes.toLong(value) <= 0;
    }
}
