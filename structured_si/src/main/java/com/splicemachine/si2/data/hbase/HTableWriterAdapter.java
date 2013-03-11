package com.splicemachine.si2.data.hbase;

import com.splicemachine.si2.data.api.SRowLock;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableWriter;
import org.apache.hadoop.hbase.client.Put;

import java.util.List;

public class HTableWriterAdapter implements STableWriter {
    private IHTableWriter writer;

    public HTableWriterAdapter(IHTableWriter writer) {
        this.writer = writer;
    }

    @Override
    public void write(STable table, Object put) {
        if (table instanceof  HbTable) {
            writer.write(((HbTable) table).table, (Put) put);
        } else {
            writer.write(((HbRegion) table).region, (Put) put);
        }
    }

    @Override
    public void write(STable table, Object put, boolean durable) {
        writer.write(table, (Put) put, durable);
    }

    @Override
    public void write(STable table, List puts) {
        writer.write(((HbTable) table).table, puts);
    }

    @Override
    public boolean checkAndPut(STable table, Object family, Object qualifier, Object value, Object put) {
        return writer.checkAndPut(((HbTable) table).table, (byte[]) family, (byte[]) qualifier, (byte[]) value, (Put) put);
    }

    @Override
    public SRowLock lockRow(STable table, Object rowKey) {
        return new HRowLock(writer.lockRow(((HbTable) table).table, (byte[]) rowKey));
    }

    @Override
    public void unLockRow(STable table, SRowLock lock) {
        writer.unLockRow(((HbTable) table).table, ((HRowLock) lock).lock);
    }
}
