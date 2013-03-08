package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.impl.sql.execute.constraint.Constraint;
import com.splicemachine.derby.impl.sql.execute.constraint.ForeignKey;
import com.splicemachine.derby.impl.sql.execute.constraint.UniqueConstraint;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class PrimaryKey extends UniqueConstraint {
    private static final Logger logger = Logger.getLogger(PrimaryKey.class);

    public PrimaryKey(){}

    @Override
    public Type getType() {
        return Type.PRIMARY_KEY;
    }

    //TODO -sf- validate Foreign Key Constraints here?

    @Override
    public String toString() {
        return "PrimaryKey";
    }
}
