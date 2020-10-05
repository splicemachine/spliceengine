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

package com.splicemachine.hbase;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.hadoop.hbase.regionserver.Store;

import java.io.IOException;

/**
 * Created by jyuan on 5/29/17.
 */
public class SpliceCompactionUtils {

    public static boolean forcePurgeDeletes(Store store) throws IOException {

        boolean prepared = false;
        SpliceTransactionResourceImpl transactionResource = null;
        Txn txn = null;
        try {
            txn = SIDriver.driver().lifecycleManager()
                    .beginTransaction();
            transactionResource = new SpliceTransactionResourceImpl();
            prepared=transactionResource.marshallTransaction(txn);
            LanguageConnectionContext lcc = transactionResource.getLcc();
            DataDictionary dd = lcc.getDataDictionary();
            String fullTableName = store.getTableName().getNameAsString();
            String[] tableNames = fullTableName.split(":");
            if (tableNames.length == 2 && tableNames[0].compareTo("splice") == 0) {
                long conglomerateId = Long.parseLong(tableNames[1]);
                ConglomerateDescriptor cd = dd.getConglomerateDescriptor(conglomerateId);
                if (cd != null) {
                    UUID tableID = cd.getTableID();
                    TableDescriptor td = dd.getTableDescriptor(tableID);
                    if (td != null)
                        return td.purgeDeletedRows();
                }
            }
        }
        catch (NumberFormatException e) {
            return false;
        }
        catch (Exception e) {
            throw new IOException(e);
        }
        finally{
            if(prepared)
                transactionResource.close();
            if (txn != null)
                txn.commit();
        }

        return false;
    }
}
