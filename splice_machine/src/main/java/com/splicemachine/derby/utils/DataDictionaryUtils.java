package com.splicemachine.derby.utils;

import com.google.common.collect.Lists;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;

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

    public static String getTableVersion(LanguageConnectionContext lcc, UUID tableId) throws StandardException {
        TableDescriptor td = getTableDescriptor(lcc, tableId);
        if (td != null) {
            return td.getVersion();
        }
        return null;
    }

    // Get 0-based columnOrdering from a table with primary key
    public static int[] getColumnOrdering(TxnView txn, UUID tableId) {

        int[] columnOrdering = null;
        try {
            SpliceTransactionResourceImpl impl = new SpliceTransactionResourceImpl();
            impl.marshallTransaction(txn);
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
        } catch (Exception e) {
            // TODO: handle exceptions
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

    public static int[] getFormatIds(TxnView txn, UUID tableId) throws SQLException, StandardException {
        SpliceTransactionResourceImpl impl = new SpliceTransactionResourceImpl();
        impl.marshallTransaction(txn);
        LanguageConnectionContext lcc = impl.getLcc();

        DataDictionary dd = lcc.getDataDictionary();
        TableDescriptor td = dd.getTableDescriptor(tableId);

        if (td != null) {
            return getFormatIds(td.getColumnDescriptorList());
        } else {
            return new int[0];
        }
    }

    public static int[] getFormatIds(ColumnDescriptorList cdList) throws StandardException {
        int[] formatIds;
        int numCols = cdList.size();
        formatIds = new int[numCols];
        for (int j = 0; j < numCols; ++j) {
            ColumnDescriptor columnDescriptor = cdList.elementAt(j);
            formatIds[j] = columnDescriptor.getType().getNull().getTypeFormatId();
        }
        return formatIds;
    }

    public static ColumnInfo[] getColumnInfo(TableDescriptor td) {
        ColumnDescriptorList cdl = td.getColumnDescriptorList();
        int len = cdl.size();
        ColumnInfo[] columnInfo = new ColumnInfo[len];
        for (int i = 0; i < len; ++i) {
            ColumnDescriptor desc = cdl.get(i);
            columnInfo[i] =
                    new ColumnInfo(desc.getColumnName(),
                                   desc.getType(),
                                   desc.getDefaultValue(),
                                   desc.getDefaultInfo(),
                                   null,
                                   desc.getDefaultUUID(),
                                   null,
                                   0,
                                   desc.getAutoincStart(),
                                   desc.getAutoincInc(),
                                   desc.getAutoinc_create_or_modify_Start_Increment());
        }
        return columnInfo;
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
