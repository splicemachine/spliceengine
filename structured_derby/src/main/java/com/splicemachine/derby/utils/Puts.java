package com.splicemachine.derby.utils;

import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

/**
 * Utility class for creating HBase puts from Derby-specific data structures.
 *
 * @author Scott Fines
 * @author John Leach
 * @author Jessie Zhang
 * Created: 1/24/13 2:00 PM
 */
public class Puts {

    private static final byte[] NULL_COLUMN_MARKER = Integer.toString(-Integer.MAX_VALUE).getBytes();
    public static final byte[] FOR_UPDATE = "U".getBytes();
    public static final String PUT_TYPE = "t";

    private Puts(){}

    /**
     * Constructs a transaction-aware update for pushing an update to a row already present in HBase.
     *
     * @param location the location of the row to update (location.getBytes() should return the HBase row key
     *                 of the row to be updated).
     * @param row the data to update
     * @param validColumns flags for which columns are to be updated,or {@code null} if all columns are to be used.
     * @param validColPositionMap a mapping between the entries in {@code validCols} and the position of the corresponding
     *                            column in {@code row}
     * @param transactionID the id of the associated transaction
     * @param serializer the Serializer to use to construct the data
     * @param extraColumns any additional metadata to be tagged to the row.
     * @return a Put representing the row to update
     * @throws IOException if {@code row} or {@code extraColumns} cannot be serialized.
     */
    public static Put buildUpdate(RowLocation location, DataValueDescriptor[] row,
                                  FormatableBitSet validColumns, int[] validColPositionMap,
                                  String transactionID, Serializer serializer, DataValueDescriptor...extraColumns)
            throws IOException{
        try {
            Put put = SpliceUtils.createPut(location.getBytes(), transactionID);
            put.setAttribute(PUT_TYPE,FOR_UPDATE);
            if(validColumns!=null){
                for(int pos = validColumns.anySetBit();pos!=-1;pos=validColumns.anySetBit(pos)){
                    int rowPos = validColPositionMap[pos];
                    addColumn(put,row[rowPos],pos-1,serializer);
                }
            }else{
                for(int i=0;i<row.length;i++){
                    addColumn(put,row[i],i,serializer);
                }
            }

            for(int pos=0;pos< extraColumns.length;pos++){
                addColumn(put,extraColumns[pos],-(pos+1),serializer);
            }
            return put;
        } catch (StandardException e) {
            throw new IOException(e);
        }
    }


    /**
	 * Constructs a transaction-aware insert for pushing data into HBase correctly, when no specific row
	 * key is required.
	 *
	 * This is useful for when it is necessary to insert a row into HBase, but not particular row key format is
	 * needed. In this case, this method will generate a random row key, and insert the row under that row key.
	 *
	 * @param row the row data to store
	 * @param transactionID the id for the associated transaction
	 * @param extraColumns any additional metadata which needs to be tagged to this row.
	 * @return a Put representing the row to insert
	 * @throws IOException if {@code row} or {@code extraColumns} cannot be serialized.
	 */
	public static Put buildInsert(DataValueDescriptor[] row, String transactionID,
																DataValueDescriptor...extraColumns) throws IOException{
		return buildInsert(SpliceUtils.getUniqueKey(),row,null,transactionID,new Serializer(),extraColumns);
	}

    /**
     * Constructs a transaction-aware insert for pushing data into HBase correctly, when no specific row
     * key is required.
     *
     * This is useful for when it is necessary to insert a row into HBase, but not particular row key format is
     * needed. In this case, this method will generate a random row key, and insert the row under that row key.
     *
     * @param row the row data to store
     * @param transactionID the id for the associated transaction
     * @param extraColumns any additional metadata which needs to be tagged to this row.
     * @return a Put representing the row to insert
     * @throws IOException if {@code row} or {@code extraColumns} cannot be serialized.
     */
    public static Put buildInsert(DataValueDescriptor[] row, String transactionID, Serializer serializer,
                                  DataValueDescriptor...extraColumns) throws IOException{
        return buildInsert(SpliceUtils.getUniqueKey(), row, null, transactionID, serializer, extraColumns);
    }

	/**
	 * Constructs a transaction-aware insert for pushing data into HBase correctly.
	 *
	 * This method will build an HBase row using the specified row key, and with columns constructed from the
	 * specified DataValueDescriptor entities (using the {@code validColumns} bitset to determine which columns should
	 * be inserted at any specific time.
	 *
	 * In addition, this method supports the adding of <em>additional columns</em>, which are not necessarily part
	 * of the row data itself, but can be used for tagging additional metadata to the row (e.g. whether the
	 * row represents the left or the right side of a join).
	 *
	 * @param rowKey the row key to use for this row
	 * @param row the data to store
	 * @param validColumns a bitset marking which columns to insert from the row
	 * @param transactionID the id of the associated transaction
	 * @param extraColumns any additionaly metadata that needs to be tagged to this row.
	 * @return a Put representing the row to insert.
	 * @throws IOException if unable to serialize any of {@code row} or {@code extraColumns} into a byte[]
	 */
	public static Put buildInsert(byte[] rowKey, DataValueDescriptor[] row, FormatableBitSet validColumns,
                                  String transactionID, DataValueDescriptor...extraColumns) throws IOException{
        return buildInsert(rowKey, row, validColumns, transactionID, new Serializer(), extraColumns);
	}

    public static Put buildTempTableInsert(byte[] rowKey, DataValueDescriptor[] row,
                                           FormatableBitSet validColumns, Serializer serializer) throws IOException {
        return buildInsert(rowKey,row,validColumns,SpliceUtils.NA_TRANSACTION_ID,serializer);
    }

    public static Put buildInsertWithSerializer(byte[] rowKey, DataValueDescriptor[] row,
                                                FormatableBitSet validColumns, String transactionId, Serializer serializer) throws IOException {
        return buildInsert(rowKey,row,validColumns,transactionId,serializer);
    }

    public static Put buildInsert(byte[] rowKey, DataValueDescriptor[] row, FormatableBitSet validColumns,
                                  String transactionID, Serializer serializer, DataValueDescriptor...extraColumns)
            throws IOException{
        Put put = SpliceUtils.createPut(rowKey, transactionID);

        if (validColumns!=null) {
            for(int i=validColumns.anySetBit(); i!=-1; i=validColumns.anySetBit(i)){
                addColumn(put,row[i],i,serializer);
            }
        } else {
           for(int i=0; i<row.length; i++){
               addColumn(put,row[i],i,serializer);
           }
        }

        for (int pos=0; pos<extraColumns.length; pos++){
            addColumn(put, extraColumns[pos], -(pos+1), serializer);
        }

        SpliceUtils.handleNullsInUpdate(put, row, validColumns);

        if(put.size()==0) {
            put.add(HBaseConstants.DEFAULT_FAMILY_BYTES, NULL_COLUMN_MARKER, new byte[]{});
        }

        return put;
    }

    /**
     * Constructs a transaction-aware insert for direct HBase actions.
     *
     * This is a convenience wrapper method around
     * {@code buildUpdate(org.apache.derby.iapi.types.RowLocation,
     * 										 org.apache.derby.iapi.types.DataValueDescriptor[],
     * 										 org.apache.derby.iapi.services.io.FormatableBitSet, byte[])}
     * for when no bitset is available, or the entire row is desired.
     *
     * @param rowKey the row key to use for this row
     * @param row the data to store
     * @param transactionID the id of the associated transaction
     * @param extraColumns additional metadata to be tagged to this row.
     * @return a Put representing the row to insert
     * @throws IOException if unable to serializer any of {@code row} into a byte[]
     */
    public static Put buildInsert(byte[] rowKey, DataValueDescriptor[] row,
                                  String transactionID,Serializer serializer,DataValueDescriptor... extraColumns) throws IOException{
        return buildInsert(rowKey, row, null, transactionID, serializer, extraColumns);
    }


	/* ****************************************************************************************************/
	/*private helper methods*/

    private static void addColumn(Put put, DataValueDescriptor descriptor, int columnNum, Serializer serializer) throws IOException {
        if(descriptor==null||descriptor.isNull())
            return; //nothing to do

        try {
            byte[] data = serializer.serialize(descriptor);
            put.add(HBaseConstants.DEFAULT_FAMILY_BYTES,Integer.toString(columnNum).getBytes(),data);
        } catch (StandardException e) {
            throw new IOException(e);
        }
    }

	private static void addColumn(Put put,DataValueDescriptor descriptor, int columnNum) throws IOException {
		if(descriptor==null||descriptor.isNull())
			return; //nothing to do

		try {
			byte[] data = DerbyBytesUtil.generateBytes(descriptor);
			put.add(HBaseConstants.DEFAULT_FAMILY_BYTES,Integer.toString(columnNum).getBytes(),data);
		} catch (StandardException e) {
			throw new IOException(e);
		}
	}

}
