package com.splicemachine.derby.utils;

import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.derby.impl.sql.execute.ColumnInfo;

import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 3/11/14
 * Time: 9:28 AM
 * To change this template use File | Settings | File Templates.
 */
public class DataDictionaryUtils {

		public static String getTableVersion(String txnId, UUID tableId) throws StandardException{
				try {
						SpliceTransactionResourceImpl impl = new SpliceTransactionResourceImpl();
						impl.marshallTransaction(txnId);
						LanguageConnectionContext lcc = impl.getLcc();

						DataDictionary dd = lcc.getDataDictionary();
						TableDescriptor td = dd.getTableDescriptor(tableId);
						return td.getVersion();
				} catch (SQLException e) {
						throw Exceptions.parseException(e);
				}
		}

    // Get 0-based columnOrdering from a table with primary key
    public static int[] getColumnOrdering(String txnId, UUID tableId) {

        int[] columnOrdering = null;
        try {
            SpliceTransactionResourceImpl impl = new SpliceTransactionResourceImpl();
            impl.marshallTransaction(txnId);
            LanguageConnectionContext lcc = impl.getLcc();

            DataDictionary dd = lcc.getDataDictionary();
            TableDescriptor td = dd.getTableDescriptor(tableId);
            ConstraintDescriptorList cdl = dd.getConstraintDescriptors(td);
            ReferencedKeyConstraintDescriptor keyDescriptor = cdl.getPrimaryKey();

            if (keyDescriptor != null) {
                int[] pkCols = keyDescriptor.getReferencedColumns();
                columnOrdering = new int[pkCols.length];
                for (int i = 0; i < pkCols.length; ++i){
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
                }
                else if (i < droppedColumnPosition - 1) {
                    newColumnOrdering[i] = columnOrdering[i];
                }
                else {
                    newColumnOrdering[i] = columnOrdering[i] - 1;
                }
            }
        }

        return newColumnOrdering;
    }

    public static int[] getFormatIds(String txnId, UUID tableId) throws SQLException, StandardException{
        int[] formatIds;

        SpliceTransactionResourceImpl impl = new SpliceTransactionResourceImpl();
        impl.marshallTransaction(txnId);
        LanguageConnectionContext lcc = impl.getLcc();

        DataDictionary dd = lcc.getDataDictionary();
        TableDescriptor td = dd.getTableDescriptor(tableId);

        ColumnDescriptorList cdList = td.getColumnDescriptorList();
        int numCols =  cdList.size();
        formatIds = new int[numCols];
        for (int j = 0; j < numCols; ++j) {
            ColumnDescriptor columnDescriptor = cdList.elementAt(j);
            formatIds[j] = columnDescriptor.getType().getNull().getTypeFormatId();
        }

        return formatIds;
    }

    public static ColumnInfo[] getColumnInfo(TableDescriptor td) {
        ColumnDescriptorList cdl = td.getColumnDescriptorList();
        int len =  cdl.size();
        ColumnInfo[] columnInfo = new ColumnInfo[len];
        for (int i = 0; i < len; ++i) {
            ColumnDescriptor desc =  (ColumnDescriptor)cdl.get(i);
            columnInfo[i] =
                    new ColumnInfo(desc.getColumnName(), desc.getType(), null, null, null, null, null, 0, 0, 0, 0);
        }
        return columnInfo;
    }

}
