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

package com.splicemachine.derby.utils;

import com.splicemachine.db.impl.sql.catalog.DataDictionaryImpl;
import splice.com.google.common.collect.Lists;

import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.SQLException;
import java.util.List;

/**
 * Static utility methods for dealing with the data dictionary and related classes.
 */
public class DataDictionaryUtils {


    public static TableDescriptor getTableDescriptor(LanguageConnectionContext lcc, UUID tableId) throws StandardException {
        DataDictionary dd = lcc.getDataDictionary();
        return dd.getTableDescriptor(tableId);
    }

    public static TableDescriptor getUncachedTableDescriptor(LanguageConnectionContext lcc, UUID tableId) throws StandardException {
        DataDictionaryImpl dd = (DataDictionaryImpl)lcc.getDataDictionary();
        return dd.getUncachedTableDescriptor(tableId);
    }

    public static String getTableVersion(LanguageConnectionContext lcc, UUID tableId) throws StandardException {
        TableDescriptor td = getTableDescriptor(lcc, tableId);
        if (td != null) {
            return td.getVersion();
        }
        return null;
    }

    /**
     * Get 0-based columnOrdering from a table with primary key when you need to
     * look up the TableDescriptor.
     * @param txn current txn
     * @param tableId the UUID of the table for which to get the column ordering
     * @return Zero-based column ordering.
     * @throws StandardException
     */
    public static int[] getColumnOrdering(TxnView txn, UUID tableId) throws StandardException {

        int[] columnOrdering = null;
        boolean prepared = false;
        SpliceTransactionResourceImpl impl = null;
        try {
            impl = new SpliceTransactionResourceImpl();
            prepared = impl.marshallTransaction(txn);

            LanguageConnectionContext lcc = impl.getLcc();
            DataDictionary dd = lcc.getDataDictionary();
            TableDescriptor td = dd.getTableDescriptor(tableId);
            ConstraintDescriptorList cdl = dd.getConstraintDescriptors(td);
            ReferencedKeyConstraintDescriptor keyDescriptor = cdl.getPrimaryKey();

            if (keyDescriptor != null) {
                int[] pkCols = keyDescriptor.getReferencedColumns();
                columnOrdering = new int[pkCols.length];
                for (int i = 0; i < pkCols.length; ++i) {
                    columnOrdering[i] = pkCols[i] - 1;
                }
            }
        } catch (SQLException e) {
            throw Exceptions.parseException(e);
        } finally {
            if (prepared)
                impl.close();
        }

        if (columnOrdering == null)
            columnOrdering = new int[0];
        return columnOrdering;
    }

    /**
     * Get 0-based columnOrdering from a table with primary key when you have a TableDescriptor.
     * @param tableDescriptor the TableDescriptor for which to get the column ordering
     * @param dd DataDictionary to gather info
     * @return Zero-based column ordering
     * @throws StandardException
     */
    public static int[] getColumnOrdering(TableDescriptor tableDescriptor, DataDictionary dd) throws StandardException {
        int[] columnOrdering = null;
        ConstraintDescriptorList cdl = dd.getConstraintDescriptors(tableDescriptor);
        ReferencedKeyConstraintDescriptor keyDescriptor = cdl.getPrimaryKey();

        if (keyDescriptor != null) {
            int[] pkCols = keyDescriptor.getReferencedColumns();
            columnOrdering = new int[pkCols.length];
            for (int i = 0; i < pkCols.length; ++i) {
                columnOrdering[i] = pkCols[i] - 1;
            }
        }
        return columnOrdering;
    }

    public static int[] getColumnOrderingAfterDropColumn(int[] columnOrdering, int droppedColumnPosition) {

        int[] newColumnOrdering = null;

        if (columnOrdering != null) {
            newColumnOrdering = new int[columnOrdering.length];
            for (int i = 0; i < columnOrdering.length; ++i) {
                if (i == droppedColumnPosition - 1) {
                    return null;
                } else if (i < droppedColumnPosition - 1) {
                    newColumnOrdering[i] = columnOrdering[i];
                } else {
                    newColumnOrdering[i] = columnOrdering[i] - 1;
                }
            }
        }

        return newColumnOrdering;
    }

    @SuppressFBWarnings(value = {"NP_ALWAYS_NULL_EXCEPTION","NP_NULL_ON_SOME_PATH"}, justification = "DB-9844")
    public static int[] getFormatIds(TxnView txn, UUID tableId) throws SQLException, StandardException {

        boolean prepared = false;
        SpliceTransactionResourceImpl transactionResource = null;
        try {
            SpliceTransactionResourceImpl impl = new SpliceTransactionResourceImpl();
            prepared = impl.marshallTransaction(txn);
            LanguageConnectionContext lcc = impl.getLcc();
            DataDictionary dd = lcc.getDataDictionary();
            TableDescriptor td = dd.getTableDescriptor(tableId);
            if (td != null) {
                return td.getFormatIds();
            } else {
                return new int[0];
            }
        } finally {
            if (prepared)
                transactionResource.close();
        }
    }

    /**
     * Given a list of ForeignKeyConstraintDescriptor return a list of conglomerate IDs for the backing
     * index associated with each foreign key.
     */
    public static List<Long> getBackingIndexConglomerateIdsForForeignKeys(List<ConstraintDescriptor> constraints) {
        List<Long> backingIndexConglomerateIds = Lists.newArrayList();
        for (ConstraintDescriptor fk : constraints) {
            ForeignKeyConstraintDescriptor foreignKeyConstraint = (ForeignKeyConstraintDescriptor) fk;
            try {
                ConglomerateDescriptor backingIndexCd = foreignKeyConstraint.getIndexConglomerateDescriptor(null);
                backingIndexConglomerateIds.add(backingIndexCd.getConglomerateNumber());
            } catch (StandardException e) {
                // not possible, called method declares but does not actually throw StandardException
                throw new RuntimeException(e);
            }
        }
        return backingIndexConglomerateIds;
    }

}
