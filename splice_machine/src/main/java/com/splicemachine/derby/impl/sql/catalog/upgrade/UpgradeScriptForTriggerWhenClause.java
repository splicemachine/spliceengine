package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.CatalogRowFactory;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.store.access.AccessFactory;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.catalog.SYSTRIGGERSRowFactory;
import com.splicemachine.db.impl.sql.catalog.TabInfoImpl;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.hbase.HBaseController;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Created by msirek on 11/5/19.
 */
public class UpgradeScriptForTriggerWhenClause extends UpgradeScriptBase {
    public UpgradeScriptForTriggerWhenClause(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        ConglomerateController heapCC = null;
        try {
            TabInfoImpl tII = sdd.getNonCoreTIByNumber(DataDictionary.SYSTRIGGERS_CATALOG_NUM);
            TableDescriptor td =
               sdd.getTableDescriptor( tII.getCatalogRowFactory().getCatalogName(),
                                       sdd.getSystemSchemaDescriptor(), tc );
            long conglomID = td.getHeapConglomerateId();
            heapCC=tc.openConglomerate(conglomID,
                                       false,0,
                                       TransactionController.MODE_RECORD,
                                       TransactionController.ISOLATION_REPEATABLE_READ);
            // If upgrade has already been done, and we somehow got here again by
            // mistake, don't re-add the WHENCLAUSETEXT column to the systriggers
            // conglomerate descriptor.
            if (heapCC instanceof HBaseController) {
                HBaseController hCC = (HBaseController)heapCC;
                if (hCC.getConglomerate().getFormat_ids().length >= 18) {
                    return;
                }
            }
            heapCC.close();
            heapCC = null;
            sdd.upgrade_addColumns(tII.getCatalogRowFactory(), new int[]{18}, tc);
            SpliceLogUtils.info(LOG, "Catalog upgraded: updated system table sys.systriggers");
        }
        catch (Exception e) {
            SpliceLogUtils.info(LOG, "Attempt to upgrade sys.systriggers failed.  Please check if it has already been upgraded and contains the correct number of columns: 18.");
        }
        finally {
            if (heapCC != null)
                heapCC.close();
        }
    }
}
