/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.DataScan;
import com.splicemachine.storage.DataScanner;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.SerializationUtils;
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
public abstract class TableScannerBuilder<V> implements Externalizable, ScanSetBuilder<V>{
    protected DataScanner scanner;
    protected ExecRow template;
    protected DataScan scan;
    protected int[] rowColumnMap;
    protected TxnView txn;
    protected int[] keyColumnEncodingOrder;
    protected int[] keyColumnTypes;
    protected int[] keyDecodingMap;
    protected int[] baseColumnMap;
    protected FormatableBitSet accessedKeys;
    protected boolean reuseRowLocation=true;
    protected String indexName;
    protected String tableDisplayName;
    protected String tableVersion;
    protected SIFilterFactory filterFactory;
    protected boolean[] keyColumnSortOrder;
    protected TransactionalRegion region;
    protected OperationContext operationContext;
    protected int[] fieldLengths;
    protected int[] columnPositionMap;
    protected long baseTableConglomId=-1l;
    protected long demarcationPoint=-1;
    protected boolean oneSplitPerRegion=false;
    protected Activation activation;
    protected MetricFactory metricFactory =Metrics.noOpMetricFactory();
    protected DataValueDescriptor optionalProbeValue;
    protected boolean pin;
    protected String delimited;
    protected String escaped;
    protected String lines;
    protected String storedAs;
    protected String location;
    protected String compression;

    @Override
    public ScanSetBuilder<V> metricFactory(MetricFactory metricFactory){
        this.metricFactory = metricFactory;
        return this;
    }

    @Override
    public ScanSetBuilder<V> activation(Activation activation){
        this.activation = activation;
        return this;
    }

    @Override
    public ScanSetBuilder<V> scanner(DataScanner scanner){
        assert scanner!=null:"Null scanners are not allowed!";
        this.scanner=scanner;
        return this;
    }

    public ScanSetBuilder<V> optionalProbeValue(DataValueDescriptor optionalProbeValue){
        assert optionalProbeValue!=null:"Null optionalProbeValues are not allowed!";
        this.optionalProbeValue=optionalProbeValue;
        return this;
    }

    @Override
    public ScanSetBuilder<V> template(ExecRow template){
        assert template!=null:"Null template rows are not allowed!";
        this.template=template;
        return this;
    }

    @Override
    public ScanSetBuilder<V> operationContext(OperationContext operationContext){
        this.operationContext=operationContext;
        return this;
    }


    @Override
    public ScanSetBuilder<V> scan(DataScan scan){
        assert scan!=null:"Null scans are not allowed!";
        this.scan=scan;
        return this;
    }

    @Override
    public ScanSetBuilder<V> transaction(TxnView txn){
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
    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public ScanSetBuilder<V> rowDecodingMap(int[] rowDecodingMap){
        assert rowDecodingMap!=null:"Null column maps are not allowed";
        this.rowColumnMap=rowDecodingMap;
        return this;
    }
    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public ScanSetBuilder<V> baseColumnMap(int[] baseColumnMap){
        assert baseColumnMap!=null:"Null column maps are not allowed";
        this.baseColumnMap=baseColumnMap;
        return this;
    }

    @Override
    public ScanSetBuilder<V> reuseRowLocation(boolean reuseRowLocation){
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
    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public ScanSetBuilder<V> keyColumnEncodingOrder(int[] keyColumnEncodingOrder){
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
    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public ScanSetBuilder<V> keyColumnSortOrder(boolean[] keyColumnSortOrder){
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
    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public ScanSetBuilder<V> keyColumnTypes(int[] keyColumnTypes){
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
    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public ScanSetBuilder<V> keyDecodingMap(int[] keyDecodingMap){
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
    @Override
    public ScanSetBuilder<V> accessedKeyColumns(FormatableBitSet accessedKeyColumns){
        this.accessedKeys=accessedKeyColumns;
        return this;
    }

    @Override
    public ScanSetBuilder<V> indexName(String indexName){
        this.indexName=indexName;
        return this;
    }

    @Override
    public ScanSetBuilder<V> tableDisplayName(String tableDisplayName){
        this.tableDisplayName=tableDisplayName;
        return this;
    }

    @Override
    public ScanSetBuilder<V> tableVersion(String tableVersion){
        this.tableVersion=tableVersion;
        return this;
    }

    public ScanSetBuilder<V> filterFactory(SIFilterFactory filterFactory){
        this.filterFactory=filterFactory;
        return this;
    }

    @Override
    public ScanSetBuilder<V> region(TransactionalRegion region){
        this.region=region;
        return this;
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public ScanSetBuilder<V> fieldLengths(int[] fieldLengths){
        this.fieldLengths=fieldLengths;
        return this;
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public ScanSetBuilder<V> columnPositionMap(int[] columnPositionMap){
        this.columnPositionMap=columnPositionMap;
        return this;
    }

    @Override
    public ScanSetBuilder<V> baseTableConglomId(long baseTableConglomId){
        this.baseTableConglomId=baseTableConglomId;
        return this;
    }

    @Override
    public ScanSetBuilder<V> demarcationPoint(long demarcationPoint){
        this.demarcationPoint=demarcationPoint;
        return this;
    }

    public ScanSetBuilder<V> oneSplitPerRegion(boolean oneSplitPerRegion){
        this.oneSplitPerRegion=oneSplitPerRegion;
        return this;
    }

    public ScanSetBuilder<V> pin(boolean pin){
        this.pin=pin;
        return this;
    }

    public ScanSetBuilder<V> delimited(String delimited){
        this.delimited=delimited;
        return this;
    }

    public ScanSetBuilder<V> escaped(String escaped){
        this.escaped=escaped;
        return this;
    }

    public ScanSetBuilder<V> lines(String lines){
        this.lines=lines;
        return this;
    }

    public ScanSetBuilder<V> storedAs(String storedAs){
        this.storedAs=storedAs;
        return this;
    }

    public ScanSetBuilder<V> location(String location){
        this.location=location;
        return this;
    }

    public ScanSetBuilder<V> compression(String compression){
        this.compression=compression;
        return this;
    }

    public SITableScanner build(){
            return new SITableScanner(
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
                    demarcationPoint,
                    optionalProbeValue);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
            out.writeObject(template);
            writeScan(out);
            out.writeBoolean(rowColumnMap!=null);
            if(rowColumnMap!=null){
                out.writeInt(rowColumnMap.length);
                //noinspection ForLoopReplaceableByForEach
                for(int i=0;i<rowColumnMap.length;++i){
                    out.writeInt(rowColumnMap[i]);
                }
            }
            writeTxn(out);
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
            out.writeBoolean(baseColumnMap!=null);
            if(baseColumnMap!=null){
                ArrayUtil.writeIntArray(out,baseColumnMap);
            }
            out.writeObject(accessedKeys);
            out.writeBoolean(reuseRowLocation);
            out.writeBoolean(oneSplitPerRegion);
            out.writeBoolean(indexName!=null);
            if(indexName!=null)
                out.writeUTF(indexName);
            out.writeBoolean(tableVersion!=null);
            if(tableVersion!=null)
                out.writeUTF(tableVersion);

            out.writeBoolean(fieldLengths!=null);
            if(fieldLengths!=null){
                out.writeInt(fieldLengths.length);
                //noinspection ForLoopReplaceableByForEach
                for(int i=0;i<fieldLengths.length;++i){
                    out.writeInt(fieldLengths[i]);
                }
                out.writeInt(columnPositionMap.length);
                //noinspection ForLoopReplaceableByForEach
                for(int i=0;i<columnPositionMap.length;++i){
                    out.writeInt(columnPositionMap[i]);
                }
                out.writeLong(baseTableConglomId);
            }
            out.writeLong(demarcationPoint);
            out.writeBoolean(optionalProbeValue !=null);
            if (optionalProbeValue!=null)
                out.writeObject(optionalProbeValue);
            out.writeBoolean(pin);
            writeNullableString(delimited,out);
            writeNullableString(escaped,out);
            writeNullableString(lines,out);
            writeNullableString(storedAs,out);
            writeNullableString(location,out);
    }
    private void writeNullableString (String nullableString,ObjectOutput out) throws IOException {
        out.writeBoolean(nullableString!=null);
        if (nullableString!=null)
            out.writeUTF(nullableString);
    }
    protected void writeTxn(ObjectOutput out) throws IOException{
        SIDriver.driver().getOperationFactory().writeTxn(txn,out);
    }

    protected void writeScan(ObjectOutput out) throws IOException{
        SIDriver.driver().getOperationFactory().writeScan(scan,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException{
            template = (ExecRow)in.readObject();
            scan=readScan(in);
            if(in.readBoolean()){
                rowColumnMap=new int[in.readInt()];
                for(int i=0;i<rowColumnMap.length;++i){
                    rowColumnMap[i]=in.readInt();
                }
            }
            txn=readTxn(in);
            keyColumnEncodingOrder=ArrayUtil.readIntArray(in);
            if(in.readBoolean()){
                keyColumnSortOrder=ArrayUtil.readBooleanArray(in);
            }
            keyColumnTypes=ArrayUtil.readIntArray(in);
            if(in.readBoolean()){
                keyDecodingMap=ArrayUtil.readIntArray(in);
            }
            if(in.readBoolean()){
                baseColumnMap=ArrayUtil.readIntArray(in);
            }
            accessedKeys=(FormatableBitSet)in.readObject();
            reuseRowLocation = in.readBoolean();
            oneSplitPerRegion = in.readBoolean();
            if(in.readBoolean())
                indexName=in.readUTF();
            if(in.readBoolean())
                tableVersion=in.readUTF();

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
            if (in.readBoolean())
                optionalProbeValue = (DataValueDescriptor) in.readObject();
            pin = in.readBoolean();
            if (in.readBoolean())
                delimited = in.readUTF();
            if (in.readBoolean())
                escaped = in.readUTF();
            if (in.readBoolean())
                lines = in.readUTF();
            if (in.readBoolean())
                storedAs = in.readUTF();
        if (in.readBoolean())
            location = in.readUTF();
    }

    protected TxnView readTxn(ObjectInput in) throws IOException{
        return SIDriver.driver().getOperationFactory().readTxn(in);
    }

    protected DataScan readScan(ObjectInput in) throws IOException{
        return SIDriver.driver().getOperationFactory().readScan(in);
    }

    @Override
    public String base64Encode() throws IOException, StandardException{
        return getTableScannerBuilderBase64String();
    }

    public static TableScannerBuilder getTableScannerBuilderFromBase64String(String base64String) throws IOException, StandardException{
        if(base64String==null)
            throw new IOException("tableScanner base64 String is null");
        return (TableScannerBuilder)SerializationUtils.deserialize(Base64.decodeBase64(base64String));
    }

    public String getTableScannerBuilderBase64String() throws IOException, StandardException{
        return Base64.encodeBase64String(SerializationUtils.serialize(this));
    }

    public DataScan getScan(){
        return scan;
    }

    @Override
    public TxnView getTxn(){
        return txn;
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

    public DataSet<V> buildDataSet(Object caller) throws StandardException {
        return buildDataSet();
    }

    @Override
    public OperationContext getOperationContext() {
        return operationContext;
    }

    public long getDemarcationPoint() {
        return this.demarcationPoint;
    }

    public DataValueDescriptor getOptionalProbeValue() {
        return this.optionalProbeValue;
    }

    public int[] getBaseColumnMap() {
        return baseColumnMap;
    }

    public long getBaseTableConglomId() {
        return baseTableConglomId;
    }

    @Override
    public int[] getColumnPositionMap() {
        return columnPositionMap;
    }

    @Override
    public ExecRow getTemplate() {
        return template;
    }


    @Override
    public boolean getPin() {
        return pin;
    }

    @Override
    public String getDelimited() {
        return delimited;
    }

    @Override
    public String getEscaped() {
        return escaped;
    }

    @Override
    public String getLines() {
        return lines;
    }

    @Override
    public String getStoredAs() {
        return storedAs;
    }

    @Override
    public String getLocation() {
        return location;
    }
}
