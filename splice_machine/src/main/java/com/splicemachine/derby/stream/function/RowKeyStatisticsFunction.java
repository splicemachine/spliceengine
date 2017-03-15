package com.splicemachine.derby.stream.function;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.types.SQLBlob;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.kvpair.KVPair;
import org.apache.hadoop.hdfs.server.namenode.HostFileManager;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.*;

/**
 * Created by jyuan on 3/14/17.
 */
public class RowKeyStatisticsFunction <Op extends SpliceOperation>
        extends SpliceFlatMapFunction<Op,Iterator<Tuple2<Long,KVPair>>, Tuple2<Long, ColumnStatisticsImpl>> implements Serializable {

    protected Map<Long, ColumnStatisticsImpl> keyStatisticsMap;
    protected List<DDLMessage.TentativeIndex> tentativeIndexList;
    protected long heapConglom;
    protected boolean initialized;
    protected SQLBlob blob;
    public RowKeyStatisticsFunction() {
    }

    public RowKeyStatisticsFunction(long heapConglom, List<DDLMessage.TentativeIndex> tentativeIndexList) {
        this.heapConglom = heapConglom;
        this.tentativeIndexList = tentativeIndexList;
    }

    @Override
    public Iterator<Tuple2<Long, ColumnStatisticsImpl>> call(Iterator<Tuple2<Long,KVPair>> mainAndIndexRows) throws Exception {

        if (!initialized) {
            init();
            initialized = true;
        }
        while (mainAndIndexRows.hasNext()) {
            Tuple2<Long, KVPair> t = mainAndIndexRows.next();
            Long conglomId = t._1;
            KVPair kvPair = t._2;
            byte[] key = kvPair.getRowKey();

            ColumnStatisticsImpl columnStatistics = keyStatisticsMap.get(conglomId);
            if (columnStatistics == null) {
                columnStatistics = new ColumnStatisticsImpl(new SQLBlob(key));
                keyStatisticsMap.put(conglomId, columnStatistics);
            }
            else {
                //blob.setValue(key);
                columnStatistics.update(new SQLBlob(key));
            }

        }
        List<Tuple2<Long, ColumnStatisticsImpl>> l = Lists.newArrayList();
        for (Long c : keyStatisticsMap.keySet()) {
            ColumnStatisticsImpl statistics = keyStatisticsMap.get(c);
            l.add(new Tuple2<>(c, statistics));
        }
        return l.iterator();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(heapConglom);
        out.writeInt(tentativeIndexList.size());
        for (DDLMessage.TentativeIndex ti: tentativeIndexList) {
            byte[] message = ti.toByteArray();
            out.writeInt(message.length);
            out.write(message);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        heapConglom = in.readLong();
        int iSize = in.readInt();
        tentativeIndexList = new ArrayList<>(iSize);
        for (int i = 0; i< iSize; i++) {
            byte[] message = new byte[in.readInt()];
            in.readFully(message);
            tentativeIndexList.add(DDLMessage.TentativeIndex.parseFrom(message));
        }
    }

    private void init() {
        keyStatisticsMap = new HashMap<>();
        blob = new SQLBlob();
    }
}
