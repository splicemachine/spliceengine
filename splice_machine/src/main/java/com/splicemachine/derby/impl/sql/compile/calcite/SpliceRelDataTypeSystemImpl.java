package com.splicemachine.derby.impl.sql.compile.calcite;

import com.splicemachine.db.iapi.reference.Limits;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;

public class SpliceRelDataTypeSystemImpl extends RelDataTypeSystemImpl {
    public static final RelDataTypeSystem INSTANCE = new SpliceRelDataTypeSystemImpl() {
    };

    @Override public int getMaxNumericPrecision() {
        return Limits.DB2_MAX_DECIMAL_PRECISION_SCALE;
    }
}
