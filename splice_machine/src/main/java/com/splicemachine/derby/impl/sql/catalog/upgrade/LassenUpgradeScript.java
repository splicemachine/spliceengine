package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.DefaultInfoImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;

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
