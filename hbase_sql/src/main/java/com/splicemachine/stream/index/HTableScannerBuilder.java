package com.splicemachine.stream.index;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iterator.DirectScanner;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.mrio.api.SpliceTableMapReduceUtil;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.DataScanner;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jyuan on 10/16/15.
 */
public class HTableScannerBuilder implements Externalizable {
    private DataScanner scanner;
    private MetricFactory metricFactory;
    private Scan scan;
    private TxnView txn;
    private	String tableVersion;
    private long demarcationPoint;
    private TransactionalRegion region;
    private int[] indexColToMainColPosMap;

    public HTableScannerBuilder scanner(DataScanner scanner) {
        assert scanner !=null :"Null scanners are not allowed!";
        this.scanner = scanner;
        return this;
    }

    public HTableScannerBuilder metricFactory(MetricFactory metricFactory) {
        this.metricFactory = metricFactory;
        return this;
    }

    public HTableScannerBuilder scan(Scan scan) {
        assert scan!=null : "Null scans are not allowed!";
        this.scan = scan;
        return this;
    }

    public HTableScannerBuilder transaction(TxnView txn){
        assert txn!=null: "No Transaction specified";
        this.txn = txn;
        return this;
    }


    public HTableScannerBuilder tableVersion(String tableVersion) {
        this.tableVersion = tableVersion;
        return this;
    }

    public HTableScannerBuilder region(TransactionalRegion region){
        this.region = region;
        return this;
    }

    public HTableScannerBuilder demarcationPoint(long demarcationPoint) {
        this.demarcationPoint = demarcationPoint;
        return this;
    }

    public HTableScannerBuilder indexColToMainColPosMap(int[] indexColToMainColPosMap) {
        this.indexColToMainColPosMap = indexColToMainColPosMap;
        return this;
    }

    public DirectScanner build(){
        return new DirectScanner(
                scanner,
                region,
                txn,
                demarcationPoint,
                metricFactory==null?Metrics.noOpMetricFactory():metricFactory);

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(SpliceTableMapReduceUtil.convertScanToString(scan));
        SIDriver.driver().getOperationFactory().writeTxn(txn, out);
        out.writeBoolean(tableVersion != null);
        if (tableVersion!=null)
            out.writeUTF(tableVersion);
        out.writeLong(demarcationPoint);
        out.writeInt(indexColToMainColPosMap.length);
        for (int i = 0; i < indexColToMainColPosMap.length; ++i) {
            out.writeInt(indexColToMainColPosMap[i]);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        scan = SpliceTableMapReduceUtil.convertStringToScan(in.readUTF());
        txn = SIDriver.driver().getOperationFactory().readTxn(in);
        if (in.readBoolean())
            tableVersion = in.readUTF();
        demarcationPoint = in.readLong();
        int len = in.readInt();
        indexColToMainColPosMap = new int[len];
        for (int i = 0; i < indexColToMainColPosMap.length; ++i) {
            indexColToMainColPosMap[i] = in.readInt();
        }
    }

    public static HTableScannerBuilder getTableScannerBuilderFromBase64String(String base64String) throws IOException, StandardException {
        if (base64String == null)
            throw new IOException("tableScanner base64 String is null");
        return (HTableScannerBuilder) SerializationUtils.deserialize(Base64.decodeBase64(base64String));
    }

    public String getTableScannerBuilderBase64String() throws IOException, StandardException {
        return Base64.encodeBase64String(SerializationUtils.serialize(this));
    }

    public Scan getScan() {
        return scan;
    }

    @Override
    public String toString() {
        return String.format("scan=%s, txn=%s, tableVerson=%s", scan, txn, tableVersion);
    }
}
