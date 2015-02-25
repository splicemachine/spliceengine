package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.SQLBoolean;

import java.sql.Types;

/**
 * @author Scott Fines
 *         Date: 2/25/15
 */
public class LassenUpgradeScript extends UpgradeScriptBase {
    public LassenUpgradeScript(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        super.upgradeSystemTables();

        //add the syscolumns descriptor
        TransactionController tc = sdd.getTransactionExecute();
        SQLBoolean template_column = new SQLBoolean();
        DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN);
        tc.addColumnToConglomerate(sdd.getSYSCOLUMNSHeapConglomerateNumber(),9, template_column,dtd.getCollationType());
    }
}
