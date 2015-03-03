package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.derby.impl.sql.catalog.SYSTABLESTATISTICSRowFactory;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.DefaultInfoImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.SQLBoolean;

import java.sql.Types;
import java.util.Collections;

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
        addStatsColumnToSysColumns(tc);

        sdd.createStatisticsTables(tc);
    }


    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void addStatsColumnToSysColumns(TransactionController tc) throws StandardException {
        //add the syscolumns descriptor
        SQLBoolean template_column = new SQLBoolean();
        DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN);
        tc.addColumnToConglomerate(sdd.getSYSCOLUMNSHeapConglomerateNumber(),9, template_column,dtd.getCollationType());

        TableDescriptor sysColumns = sdd.getTableDescriptor("SYSCOLUMNS",sdd.getSystemSchemaDescriptor(),tc);
        UUID defaultUuid = sdd.getUUIDFactory().createUUID();

        ColumnDescriptor cd = new ColumnDescriptor("COLLECTSTATS",10,
                dtd,
                template_column,
                new DefaultInfoImpl(false,null,null),
                sysColumns,
                defaultUuid,
                0,
                0);
        sdd.addDescriptor(cd,sysColumns, DataDictionary.SYSCOLUMNS_CATALOG_NUM,false,tc);

        sysColumns.getColumnDescriptorList().add(cd);
        sdd.updateSYSCOLPERMSforAddColumnToUserTable(sysColumns.getUUID(), tc);
    }


}
