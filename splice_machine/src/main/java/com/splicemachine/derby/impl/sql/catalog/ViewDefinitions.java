/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.catalog.*;
import com.splicemachine.utils.Pair;

import java.util.HashMap;
import java.util.Map;

class SystemViewDefinitions {
    static private class ViewInfo {
        int catalogNum;
        int viewIndex;
        String viewSql;

        ViewInfo(int catalogNum, int viewIndex, String viewSql) {
            this.catalogNum = catalogNum;
            this.viewIndex = viewIndex;
            this.viewSql = viewSql;
        }
    }

    Map<Pair<String, String>, ViewInfo> views;

    SystemViewDefinitions() {
        this.views = new HashMap<>();
        views.put(new Pair<>("SYSVW", "SYSALLROLES"), new ViewInfo(DataDictionary.SYSROLES_CATALOG_NUM, 0, SYSROLESRowFactory.ALLROLES_VIEW_SQL));
        views.put(new Pair<>("SYSVW", "SYSSCHEMASVIEW"), new ViewInfo(DataDictionary.SYSSCHEMAS_CATALOG_NUM, 0, null)); // sql determined at run time
        views.put(new Pair<>("SYSVW", "SYSCONGLOMERATEINSCHEMAS"), new ViewInfo(DataDictionary.SYSCONGLOMERATES_CATALOG_NUM, 0, SYSCONGLOMERATESRowFactory.SYSCONGLOMERATE_IN_SCHEMAS_VIEW_SQL));
        views.put(new Pair<>("SYSVW", "SYSTABLESVIEW"), new ViewInfo(DataDictionary.SYSTABLES_CATALOG_NUM, 0, SYSTABLESRowFactory.SYSTABLE_VIEW_SQL));
        views.put(new Pair<>("SYSVW", "SYSCOLUMNSVIEW"), new ViewInfo(DataDictionary.SYSCOLUMNS_CATALOG_NUM, 0, SYSCOLUMNSRowFactory.SYSCOLUMNS_VIEW_SQL));
        views.put(new Pair<>("SYSVW", "SYSTABLESTATISTICS"), new ViewInfo(DataDictionary.SYSTABLESTATS_CATALOG_NUM, 0, SYSTABLESTATISTICSRowFactory.STATS_VIEW_SQL));
        views.put(new Pair<>("SYSVW", "SYSCOLUMNSTATISTICS"), new ViewInfo(DataDictionary.SYSCOLUMNSTATS_CATALOG_NUM, 0, SYSCOLUMNSTATISTICSRowFactory.STATS_VIEW_SQL));
        views.put(new Pair<>("SYSVW", "SYSTABLEPERMSVIEW"), new ViewInfo(DataDictionary.SYSTABLEPERMS_CATALOG_NUM, 0, SYSTABLEPERMSRowFactory.SYSTABLEPERMS_VIEW_SQL));
        views.put(new Pair<>("SYSVW", "SYSSCHEMAPERMSVIEW"), new ViewInfo(DataDictionary.SYSSCHEMAPERMS_CATALOG_NUM, 0, SYSSCHEMAPERMSRowFactory.SYSSCHEMAPERMS_VIEW_SQL));
        views.put(new Pair<>("SYSVW", "SYSCOLPERMSVIEW"), new ViewInfo(DataDictionary.SYSCOLPERMS_CATALOG_NUM, 0, SYSCOLPERMSRowFactory.SYSCOLPERMS_VIEW_SQL));
        views.put(new Pair<>("SYSVW", "SYSROUTINEPERMSVIEW"), new ViewInfo(DataDictionary.SYSROUTINEPERMS_CATALOG_NUM, 0, SYSROUTINEPERMSRowFactory.SYSROUTINEPERMS_VIEW_SQL));
        views.put(new Pair<>("SYSVW", "SYSPERMSVIEW"), new ViewInfo(DataDictionary.SYSPERMS_CATALOG_NUM, 0, SYSPERMSRowFactory.SYSPERMS_VIEW_SQL));
        views.put(new Pair<>("SYSVW", "SYSALIASTOTABLEVIEW"), new ViewInfo(DataDictionary.SYSALIASES_CATALOG_NUM, 0, SYSALIASESRowFactory.SYSALIAS_TO_TABLE_VIEW_SQL));

        views.put(new Pair<>("SYSIBM", "SYSCOLUMNS"), new ViewInfo(DataDictionary.SYSCOLUMNS_CATALOG_NUM, 1, SYSCOLUMNSRowFactory.SYSCOLUMNS_VIEW_IN_SYSIBM));
        views.put(new Pair<>("SYSIBM", "SYSTABLES"), new ViewInfo(DataDictionary.SYSTABLES_CATALOG_NUM, 1, SYSTABLESRowFactory.SYSTABLES_VIEW_IN_SYSIBM));
        views.put(new Pair<>("SYSIBM", "SYSKEYCOLUSE"), new ViewInfo(DataDictionary.SYSCONSTRAINTS_CATALOG_NUM, 0, SYSCONSTRAINTSRowFactory.SYSKEYCOLUSE_VIEW_IN_SYSIBM));
        views.put(new Pair<>("SYSIBM", "SYSINDEXES"), new ViewInfo(DataDictionary.SYSCONGLOMERATES_CATALOG_NUM, 2, SYSCONGLOMERATESRowFactory.SYSIBM_SYSINDEXES_VIEW_SQL));

        views.put(new Pair<>("SYSCAT", "INDEXCOLUSE"), new ViewInfo(DataDictionary.SYSCONGLOMERATES_CATALOG_NUM, 1, SYSCONGLOMERATESRowFactory.SYSCAT_INDEXCOLUSE_VIEW_SQL));
        views.put(new Pair<>("SYSCAT", "REFERENCES"), new ViewInfo(DataDictionary.SYSFOREIGNKEYS_CATALOG_NUM, 0, SYSFOREIGNKEYSRowFactory.SYSCAT_REFERENCES_VIEW_SQL));
        //views.put(new Pair<>("SYSCAT", "COLUMNS"), new ViewInfo(DataDictionary.SYSCOLUMNS_CATALOG_NUM, 2, SYSCOLUMNSRowFactory.COLUMNS_VIEW_IN_SYSCAT));

        views.put(new Pair<>("SYSIBMADM", "SNAPAPPL"), new ViewInfo(DataDictionary.SYSMONGETCONNECTION_CATALOG_NUM, 0, SYSMONGETCONNECTIONRowFactory.SNAPAPPL_VIEW_SQL));
        views.put(new Pair<>("SYSIBMADM", "SNAPAPPL_INFO"), new ViewInfo(DataDictionary.SYSMONGETCONNECTION_CATALOG_NUM, 1, SYSMONGETCONNECTIONRowFactory.SNAPAPPL_INFO_VIEW_SQL));
        views.put(new Pair<>("SYSIBMADM", "APPLICATIONS"), new ViewInfo(DataDictionary.SYSMONGETCONNECTION_CATALOG_NUM, 2, SYSMONGETCONNECTIONRowFactory.APPLICATIONS_VIEW_SQL));
    }

    void createOrUpdateView(TransactionController tc, SpliceDataDictionary dd, String schemaName, String viewName) throws StandardException {
        SchemaDescriptor schemaDesc = dd.getSystemWideSchemaDescriptor(schemaName);
        ViewInfo info = views.get(new Pair<>(schemaName, viewName));
        // Runtime handling for sysschemasview:
        String viewSql = (schemaName.equals("SYSVW") && viewName.equals("SYSSCHEMASVIEW")) ? dd.getSchemaViewSQL() : info.viewSql;

        dd.createOrUpdateSystemView(tc, schemaDesc, info.catalogNum, viewName, info.viewIndex, viewSql);
    }

}
