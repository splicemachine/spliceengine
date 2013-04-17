package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.SRowLock;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.api.STableWriter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.util.List;

public class HTableWriterAdapter implements STableWriter {
    private IHTableWriter writer;

    public HTableWriterAdapter(IHTableWriter writer) {
        this.writer = writer;
    }

    @Override
    public void write(STable table, Object put) {
        if (table instanceof HbTable) {
            writer.write(((HbTable) table).table, (Put) put);
        } else {
            writer.write(((HbRegion) table).region, (Put) put);
        }
    }

    @Override
    public void write(STable table, Object put, SRowLock rowLock) {
        if (table instanceof HbTable) {
            writer.write(((HbTable) table).table, (Put) put);
        } else {
            writer.write(((HbRegion) table).region, (Put) put, ((HRowLock) rowLock).regionRowLock);
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
    public SRowLock lockRow(STable table, Object rowKey) {
        if (table instanceof HbTable) {
            return new HRowLock(writer.lockRow(((HbTable) table).table, (byte[]) rowKey));
        } else {
            final HRegion region = ((HbRegion) table).region;
            final Integer lock = writer.lockRow(region, (byte[]) rowKey);
            if (lock == null) {
                throw new RuntimeException("Unable to obtain row lock on region of table " + region.getTableDesc().getNameAsString());
            }
            return new HRowLock(lock);
        }
    }

    @Override
    public void unLockRow(STable table, SRowLock lock) {
        if (table instanceof HbTable) {
            writer.unLockRow(((HbTable) table).table, ((HRowLock) lock).lock);
        } else {
            writer.unLockRow(((HbRegion) table).region, ((HRowLock) lock).regionRowLock);
        }
    }
}
