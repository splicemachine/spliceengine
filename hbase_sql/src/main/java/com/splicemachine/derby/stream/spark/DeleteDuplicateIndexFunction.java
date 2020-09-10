package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import splice.com.google.common.collect.Lists;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.List;

public class DeleteDuplicateIndexFunction<Op extends SpliceOperation> extends SpliceFlatMapFunction<Op,
        Iterator<Tuple2<String, Tuple2<List<Tuple2<byte[], ExecRow>>, ExecRow>>>, Tuple2<String, Tuple2<byte[], ExecRow>>> {

    private long conglomerate;
    private TxnView txn;
    private DDLMessage.TentativeIndex tentativeIndex;
    private int[] baseColumnMap;
    private boolean fix;
    RecordingCallBuffer<KVPair> writeBuffer;

    public DeleteDuplicateIndexFunction(){}

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public DeleteDuplicateIndexFunction(long conglomerate, TxnView txn, DDLMessage.TentativeIndex tentativeIndex,
                                        int[] baseColumnMap, boolean fix) throws IOException{
        this.conglomerate = conglomerate;
        this.txn = txn;
        this.tentativeIndex = tentativeIndex;
        this.baseColumnMap = baseColumnMap;
        this.fix = fix;
    }

    public Iterator<Tuple2<String, Tuple2<byte[], ExecRow>>> call(Iterator<Tuple2<String, Tuple2<List<Tuple2<byte[], ExecRow>>, ExecRow>>> iterator) throws Exception {
        if (fix) {
            WriteCoordinator writeCoordinator = PipelineDriver.driver().writeCoordinator();
            WriteConfiguration writeConfiguration = writeCoordinator.defaultWriteConfiguration();
            Partition indexPartition = SIDriver.driver().getTableFactory().getTable(Long.toString(conglomerate));
            writeBuffer = writeCoordinator.writeBuffer(indexPartition, txn, null, writeConfiguration);
        }

        List<Tuple2<String, Tuple2<byte[], ExecRow>>> result = Lists.newArrayList();
        List<Integer> indexToMain = tentativeIndex.getIndex().getIndexColsToMainColMapList();
        while (iterator.hasNext()) {
            Tuple2<String, Tuple2<List<Tuple2<byte[], ExecRow>>, ExecRow>> tuple2 = iterator.next();
            Tuple2<List<Tuple2<byte[], ExecRow>>, ExecRow> t = tuple2._2;
            ExecRow baseRow = t._2;
            List<Tuple2<byte[], ExecRow>> indexes = t._1;
            for (Tuple2<byte[], ExecRow> index : indexes) {
                boolean duplicate = false;
                DataValueDescriptor[] dvds = index._2.getRowArray();
                for (int i = 0; i < dvds.length - 1; ++i) {
                    int col = baseColumnMap[indexToMain.get(i) - 1];
                    if (!dvds[i].equals(baseRow.getColumn(col + 1))) {
                        duplicate = true;
                        break;
                    }
                }
                if (duplicate) {
                    if (fix) {
                        writeBuffer.add(new KVPair(index._1, new byte[0], KVPair.Type.DELETE));
                    }
                    result.add(new Tuple2(tuple2._1, index));
                }
            }
        }
        if (fix) {
            writeBuffer.flushBuffer();
        }
        return result.iterator();
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(conglomerate);
        SIDriver.driver().getOperationFactory().writeTxn(txn,out);
        byte[] message = tentativeIndex.toByteArray();
        out.writeInt(message.length);
        out.write(message);
        ArrayUtil.writeIntArray(out, baseColumnMap);
        out.writeBoolean(fix);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        conglomerate = in.readLong();
        txn = SIDriver.driver().getOperationFactory().readTxn(in);
        byte[] message = new byte[in.readInt()];
        in.readFully(message);
        tentativeIndex = DDLMessage.TentativeIndex.parseFrom(message);
        baseColumnMap = ArrayUtil.readIntArray(in);
        fix = in.readBoolean();
    }
}
