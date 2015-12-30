package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.shared.common.udt.UDTBase;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.stats.StatisticsScanner;
import com.splicemachine.mrio.api.SpliceTableMapReduceUtil;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.DataScan;
import com.splicemachine.storage.DataScanner;
import org.apache.commons.lang.SerializationUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Companion Builder class for SITableScanner
 *
 * @author Scott Fines
 *         Date: 4/9/14
 */
public class TableScannerBuilder implements Externalizable{
    protected DataScanner scanner;
    protected ExecRow template;
    protected DataScan scan;
    protected int[] rowColumnMap;
    protected TxnView txn;
    protected int[] keyColumnEncodingOrder;
    protected int[] keyColumnTypes;
    protected int[] keyDecodingMap;
    protected FormatableBitSet accessedKeys;
    protected boolean reuseRowLocation=true;
    protected String indexName;
    protected String tableVersion;
    protected SIFilterFactory filterFactory;
    protected boolean[] keyColumnSortOrder;
    protected TransactionalRegion region;
    protected int[] execRowTypeFormatIds;
    protected OperationContext operationContext;
    protected int[] fieldLengths;
    protected int[] columnPositionMap;
    protected long baseTableConglomId=-1l;
    protected long demarcationPoint=-1;

    public TableScannerBuilder scanner(DataScanner scanner){
        assert scanner!=null:"Null scanners are not allowed!";
        this.scanner=scanner;
        return this;
    }

    public TableScannerBuilder template(ExecRow template){
        assert template!=null:"Null template rows are not allowed!";
        this.template=template;
        return this;
    }

    public TableScannerBuilder operationContext(OperationContext operationContext){
        this.operationContext=operationContext;
        return this;
    }


    public TableScannerBuilder scan(DataScan scan){
        assert scan!=null:"Null scans are not allowed!";
        this.scan=scan;
        return this;
    }

    public TableScannerBuilder execRowTypeFormatIds(int[] execRowTypeFormatIds){
        assert execRowTypeFormatIds!=null:"Null ExecRow formatIDs are not allowed!";
        this.execRowTypeFormatIds=execRowTypeFormatIds;
        return this;
    }

    public TableScannerBuilder transaction(TxnView txn){
        assert txn!=null:"No Transaction specified";
        this.txn=txn;
        return this;
    }

    /**
     * Set the row decoding map.
     * <p/>
     * For example, if your row is (a,b,c,d), and the key columns are (c,a).Now, suppose
     * that you are returning rows (a,c,d); then, the row decoding map would be [-1,-1,-1,2] (d's position
     * in the entire row is 3, so it has to be located at that index, and it's location in the decoded row is 2,
     * so that's the value).
     * <p/>
     * Note that the row decoding map should be -1 for all row elements which are kept in the key.
     *
     * @param rowDecodingMap the map for decoding the row values.
     * @return a Builder with the rowDecodingMap set.
     */
    public TableScannerBuilder rowDecodingMap(int[] rowDecodingMap){
        assert rowDecodingMap!=null:"Null column maps are not allowed";
        this.rowColumnMap=rowDecodingMap;
        return this;
    }

    public TableScannerBuilder reuseRowLocation(boolean reuseRowLocation){
        this.reuseRowLocation=reuseRowLocation;
        return this;
    }


    /**
     * Set the encoding order for the key columns.
     * <p/>
     * For example, if your row is (a,b,c,d), the keyColumnOrder is as follows
     * <p/>
     * 1. keys = (a)   => keyColumnEncodingOrder = [0]
     * 2. keys = (a,b) => keyColumnEncodingOrder = [0,1]
     * 3. keys = (a,c) => keyColumnEncodingOrder = [0,2]
     * 4. keys = (c,a) => keyColumnEncodingOrder = [2,0]
     * <p/>
     * So, in general, the keyColumnEncodingOrder is the order in which keys are encoded,
     * referencing their column position IN THE ENTIRE ROW.
     *
     * @param keyColumnEncodingOrder the order in which keys are encoded, referencing their column
     *                               position in the ENTIRE ROW.
     * @return a Builder with the keyColumnEncodingOrder set
     */
    public TableScannerBuilder keyColumnEncodingOrder(int[] keyColumnEncodingOrder){
        this.keyColumnEncodingOrder=keyColumnEncodingOrder;
        return this;
    }

    /**
     * Set the sort order for the key columns, IN THE ORDER THEY ARE ENCODED.
     * <p/>
     * That is, if the table is (a,b,c,d) and the key is (a asc,c desc,b desc), then
     * {@code keyColumnEncodingOrder = [0,2,1]}, and {@code keyColumnSortOrder = [true,false,false]}
     *
     * @param keyColumnSortOrder the sort order of each key, in the order in which keys are encoded.
     * @return a builder with keyColumnSortOrder set
     */
    public TableScannerBuilder keyColumnSortOrder(boolean[] keyColumnSortOrder){
        this.keyColumnSortOrder=keyColumnSortOrder;
        return this;
    }

    /**
     * Set the types of the key columns, IN THE ORDER IN WHICH THEY ARE ENCODED.
     * So if the keyColumnEncodingOrder = [2,0], then keyColumnTypes[0] should be the type
     * of column 2, while keyColumnTypes[1] is the type of column 0.
     *
     * @param keyColumnTypes the data types for ALL key columns, in the order the keys were encoded
     * @return a Builder with the key column types set
     */
    public TableScannerBuilder keyColumnTypes(int[] keyColumnTypes){
        this.keyColumnTypes=keyColumnTypes;
        return this;
    }

    /**
     * Specify the location IN THE DESTINATION ROW where the key columns are intended.
     * <p/>
     * For example, Suppose you are scanning a row (a,b,c,d), and the key is (c,a). Now
     * say you want to return (a,c,d). Then your keyColumnEncodingOrder is [2,0], and
     * your keyDecodingMap is [1,0] (the first entry is 1 because the destination location of
     * column c is 1; the second entry is 0 because a is located in position 0 in the main row).
     *
     * @param keyDecodingMap the map from the keyColumnEncodingOrder to the location in the destination
     *                       row.
     * @return a Builder with the key decoding map set.
     */
    public TableScannerBuilder keyDecodingMap(int[] keyDecodingMap){
        this.keyDecodingMap=keyDecodingMap;
        return this;
    }

    /**
     * Specify which key columns IN THE ENCODING ORDER are to be decoded.
     * <p/>
     * For example, suppose you are scanning a row (a,b,c,d) with a key of (c,a). Now say
     * you want to return (a,b,d). Then accessedKeyColumns = {1}, because 1 is the location IN THE KEY
     * of the column of interest.
     * <p/>
     * This can be constructed if you have {@code keyColumnEncodingOrder} and {@code keyDecodingMap},
     * as defined by:
     * <p/>
     * for(int i=0;i<keyColumnEncodingOrder.length;i++){
     * int decodingPosition = keyDecodingMap[keyColumnEncodingOrder[i]];
     * if(decodingPosition>=0)
     * accessedKeyColumns.set(i);
     * }
     * <p/>
     * Note: the above assumes that keyDecodingMap has a negative number when key columns are not interesting to us.
     *
     * @param accessedKeyColumns the keys which are to be decoded, IN THE KEY ENCODING ORDER.
     * @return a Builder with the accessedKeyColumns set.
     */
    public TableScannerBuilder accessedKeyColumns(FormatableBitSet accessedKeyColumns){
        this.accessedKeys=accessedKeyColumns;
        return this;
    }

    public TableScannerBuilder indexName(String indexName){
        this.indexName=indexName;
        return this;
    }

    public TableScannerBuilder tableVersion(String tableVersion){
        this.tableVersion=tableVersion;
        return this;
    }

    public TableScannerBuilder filterFactory(SIFilterFactory filterFactory){
        this.filterFactory=filterFactory;
        return this;
    }

    public TableScannerBuilder region(TransactionalRegion region){
        this.region=region;
        return this;
    }

    public TableScannerBuilder fieldLengths(int[] fieldLengths){
        this.fieldLengths=fieldLengths;
        return this;
    }

    public TableScannerBuilder columnPositionMap(int[] columnPositionMap){
        this.columnPositionMap=columnPositionMap;
        return this;
    }

    public TableScannerBuilder baseTableConglomId(long baseTableConglomId){
        this.baseTableConglomId=baseTableConglomId;
        return this;
    }

    public TableScannerBuilder demarcationPoint(long demarcationPoint){
        this.demarcationPoint=demarcationPoint;
        return this;
    }

    public SITableScanner build(){
        if(fieldLengths!=null){
            return new StatisticsScanner(
                    SIDriver.driver().getDataLib(),
                    scanner,
                    region,
                    template,
                    scan,
                    rowColumnMap,
                    txn,
                    keyColumnEncodingOrder,
                    keyColumnSortOrder,
                    keyColumnTypes,
                    keyDecodingMap,
                    accessedKeys,
                    reuseRowLocation,
                    indexName,
                    tableVersion,
                    filterFactory,
                    fieldLengths,
                    columnPositionMap);
        }else{
            return new SITableScanner(
                    SIDriver.driver().getDataLib(),
                    scanner,
                    region,
                    template,
                    scan,
                    rowColumnMap,
                    txn,
                    keyColumnEncodingOrder,
                    keyColumnSortOrder,
                    keyColumnTypes,
                    keyDecodingMap,
                    accessedKeys,
                    reuseRowLocation,
                    indexName,
                    tableVersion,
                    filterFactory,
                    demarcationPoint);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        try{
            out.writeBoolean(execRowTypeFormatIds!=null);
            if(execRowTypeFormatIds!=null){
                out.writeInt(execRowTypeFormatIds.length);
                for(int i=0;i<execRowTypeFormatIds.length;++i){
                    out.writeInt(execRowTypeFormatIds[i]);
                    if(execRowTypeFormatIds[i]==StoredFormatIds.SQL_USERTYPE_ID_V3){
                        DataValueDescriptor dvd=template.getColumn(i+1);
                        Object o=null;
                        if(dvd!=null){
                            o=dvd.getObject();
                        }
                        if(o!=null && o instanceof UDTBase){
                            // Serialize this UDT or UDA
                            out.writeBoolean(true);
                            out.writeObject(o);
                        }else{
                            out.writeBoolean(false);
                        }
                    }
                }
            }
            out.writeUTF(SpliceTableMapReduceUtil.convertScanToString(scan));
            out.writeBoolean(rowColumnMap!=null);
            if(rowColumnMap!=null){
                out.writeInt(rowColumnMap.length);
                for(int i=0;i<rowColumnMap.length;++i){
                    out.writeInt(rowColumnMap[i]);
                }
            }
            TransactionOperations.getOperationFactory().writeTxn(txn,out);
            ArrayUtil.writeIntArray(out,keyColumnEncodingOrder);
            out.writeBoolean(keyColumnSortOrder!=null);
            if(keyColumnSortOrder!=null){
                ArrayUtil.writeBooleanArray(out,keyColumnSortOrder);
            }
            ArrayUtil.writeIntArray(out,keyColumnTypes);
            out.writeBoolean(keyDecodingMap!=null);
            if(keyDecodingMap!=null){
                ArrayUtil.writeIntArray(out,keyDecodingMap);
            }
            out.writeObject(accessedKeys);
            out.writeBoolean(indexName!=null);
            if(indexName!=null)
                out.writeUTF(indexName);
            out.writeBoolean(tableVersion!=null);
            if(tableVersion!=null)
                out.writeUTF(tableVersion);
            out.writeBoolean(operationContext!=null);
            if(operationContext!=null)
                out.writeObject(operationContext);

            out.writeBoolean(fieldLengths!=null);
            if(fieldLengths!=null){
                out.writeInt(fieldLengths.length);
                for(int i=0;i<fieldLengths.length;++i){
                    out.writeInt(fieldLengths[i]);
                }
                out.writeInt(columnPositionMap.length);
                for(int i=0;i<columnPositionMap.length;++i){
                    out.writeInt(columnPositionMap[i]);
                }
                out.writeLong(baseTableConglomId);
            }
            out.writeLong(demarcationPoint);
        }catch(StandardException e){
            throw new IOException(e.getCause());
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException{
        try{
            if(in.readBoolean()){
                int n=in.readInt();
                execRowTypeFormatIds=new int[n];
                template=new ValueRow(n);
                DataValueDescriptor[] rowArray=template.getRowArray();
                for(int i=0;i<n;++i){
                    execRowTypeFormatIds[i]=in.readInt();
                    rowArray[i]=LazyDataValueFactory.getLazyNull(execRowTypeFormatIds[i]);

                    if(execRowTypeFormatIds[i]==StoredFormatIds.SQL_USERTYPE_ID_V3){
                        if(in.readBoolean()){
                            Object o=in.readObject();
                            rowArray[i].setValue(o);
                        }
                    }
                }
            }
            scan=SpliceTableMapReduceUtil.convertStringToScan(in.readUTF());
            if(in.readBoolean()){
                rowColumnMap=new int[in.readInt()];
                for(int i=0;i<rowColumnMap.length;++i){
                    rowColumnMap[i]=in.readInt();
                }
            }
            txn=TransactionOperations.getOperationFactory().readTxn(in);
            keyColumnEncodingOrder=ArrayUtil.readIntArray(in);
            if(in.readBoolean()){
                keyColumnSortOrder=ArrayUtil.readBooleanArray(in);
            }
            keyColumnTypes=ArrayUtil.readIntArray(in);
            if(in.readBoolean()){
                keyDecodingMap=ArrayUtil.readIntArray(in);
            }
            accessedKeys=(FormatableBitSet)in.readObject();
            if(in.readBoolean())
                indexName=in.readUTF();
            if(in.readBoolean())
                tableVersion=in.readUTF();
            if(in.readBoolean())
                operationContext=(OperationContext)in.readObject();

            if(in.readBoolean()){
                int n=in.readInt();
                fieldLengths=new int[n];
                for(int i=0;i<n;++i){
                    fieldLengths[i]=in.readInt();
                }
                n=in.readInt();
                columnPositionMap=new int[n];
                for(int i=0;i<n;++i){
                    columnPositionMap[i]=in.readInt();
                }
                baseTableConglomId=in.readLong();
            }
            demarcationPoint=in.readLong();
        }catch(StandardException e){
            throw new IOException(e.getCause());
        }
    }

    public static TableScannerBuilder getTableScannerBuilderFromBase64String(String base64String) throws IOException, StandardException{
        if(base64String==null)
            throw new IOException("tableScanner base64 String is null");
        return (TableScannerBuilder)SerializationUtils.deserialize(Base64.decode(base64String));
    }

    public String getTableScannerBuilderBase64String() throws IOException, StandardException{
        return Base64.encodeBytes(SerializationUtils.serialize(this));
    }

    public DataScan getScan(){
        return scan;
    }

    @Override
    public String toString(){
        return String.format("template=%s, scan=%s, rowColumnMap=%s, txn=%s, "
                        +"keyColumnEncodingOrder=%s, keyColumnSortOrder=%s, keyColumnTypes=%s, keyDecodingMap=%s, "
                        +"accessedKeys=%s, indexName=%s, tableVerson=%s",
                template,scan,rowColumnMap!=null?Arrays.toString(rowColumnMap):"NULL",
                txn,
                keyColumnEncodingOrder!=null?Arrays.toString(keyColumnEncodingOrder):"NULL",
                keyColumnSortOrder!=null?Arrays.toString(keyColumnSortOrder):"NULL",
                keyColumnTypes!=null?Arrays.toString(keyColumnTypes):"NULL",
                keyDecodingMap!=null?Arrays.toString(keyDecodingMap):"NULL",
                accessedKeys,indexName,tableVersion);
    }

    public int[] getExecRowTypeFormatIds(){
        return execRowTypeFormatIds;
    }
}
