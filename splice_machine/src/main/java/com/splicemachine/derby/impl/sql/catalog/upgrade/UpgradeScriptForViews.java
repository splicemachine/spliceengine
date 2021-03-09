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

package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.utils.Pair;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;

/**
 */
public class UpgradeScriptForViews {
    static public class UpgradeScriptForViewsInternal extends UpgradeScriptBase {
        final String[] views;
        final String name;

        @SuppressFBWarnings("EI_EXPOSE_REP2")
        public UpgradeScriptForViewsInternal(String name, String[] views,
                                             SpliceDataDictionary sdd, TransactionController tc) {
            super(sdd, tc);
            this.views = views;
            this.name = name;
        }

        @Override
        protected void upgradeSystemTables() throws StandardException {
            for (String p : views) {
                String[] split = p.split("\\.");
                assert split.length == 2;
                sdd.createOrUpdateSystemView(tc, split[0], split[1]);
            }
        }

        public String toString() {
            return "UpgradeScriptForViews " + name;
        }
    }

    public static UpgradeScriptBase changingGetKeyColumnPosition(SpliceDataDictionary sdd, TransactionController tc) {
        return new UpgradeScriptForViewsInternal("ChangingGetKeyColumnPosition", new String[]{
                    "SYSIBM.SYSCOLUMNS",
                    "SYSCAT.INDEXCOLUSE",
                    "SYSIBM.SYSKEYCOLUSE",
                    "SYSIBM.SYSTABLES"
                    }, sdd, tc);
    }

    // DB-11610 fix NPE in select * from sysvw.SYSCOLPERMSVIEW
    public static UpgradeScriptBase fixSYSCOLPERMSVIEW(SpliceDataDictionary sdd, TransactionController tc) {
        return new UpgradeScriptForViewsInternal("FixSYSCOLPERMSVIEW", new String[]{
                    "SYSVW.SYSCOLPERMSVIEW"
            }, sdd, tc);
    }

    // newly created for DB-11267
    public static UpgradeScriptBase addViewsDB11267(SpliceDataDictionary sdd, TransactionController tc) {
        return new UpgradeScriptForViewsInternal("AddViewsDB_11267", new String[]{
                "SYSVW.SYSCONGLOMERATESVIEW",
                "SYSVW.SYSDEPENDSVIEW",
                "SYSVW.SYSSEQUENCESVIEW"
        }, sdd, tc);
    }
}
