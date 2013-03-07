package com.splicemachine.derby.impl.sql.execute.constraint;

import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.impl.sql.execute.index.TableSource;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.catalog.IndexDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class UniqueConstraint implements Constraint {
    private static final Logger logger = Logger.getLogger(UniqueConstraint.class);

    @Override
    public boolean validate(Put put,RegionCoprocessorEnvironment rce) throws IOException {
         /*
         * If the put is tagged as an update, we don't validate.
         *
         * This is because updates either change the row key or they do not.
         *
         * If the row key is changed, the caller must delete the old row and insert the new,
         * which will pass back through this as an Insert Put type. If the row key is not
         * changed, then we have nothing to worry about, so just ignore the validation.
         */
        if(Arrays.equals(put.getAttribute(Puts.PUT_TYPE),Puts.FOR_UPDATE)) return true;

        SpliceLogUtils.trace(logger, "Validating local put");
        Get get = new Get(put.getRow());
        get.addFamily(HBaseConstants.DEFAULT_FAMILY_BYTES);

        Result result = rce.getRegion().get(get,null);

        boolean rowPresent = result!=null && !result.isEmpty();
        SpliceLogUtils.trace(logger,rowPresent? "row exists!": "row not yet present");
        if(rowPresent)
            SpliceLogUtils.trace(logger,result.toString());
        return !rowPresent;
    }

    @Override
    public boolean validate(Delete delete,RegionCoprocessorEnvironment rce) throws IOException {
        //no need to check anything
        return true;
    }

    public Type getType(){
        return Type.UNIQUE;
    }

    public static Constraint create() {
        return new UniqueConstraint();
    }

}
