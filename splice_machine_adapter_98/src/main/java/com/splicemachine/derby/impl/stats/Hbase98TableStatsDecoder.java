package com.splicemachine.derby.impl.stats;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.storage.EntryDecoder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

/**
 * @author Scott Fines
 *         Date: 3/31/15
 */
public class Hbase98TableStatsDecoder extends TableStatsDecoder{
    @Override
    protected void setKeyInDecoder(Result result,MultiFieldDecoder cachedDecoder){
        assert result!=null && result.size()>0: "Programmer error: Should not be called with an empty result!";
        Cell[] cells=result.rawCells();
        Cell c = cells[0];
        cachedDecoder.set(c.getRowArray(),c.getRowOffset(),c.getRowLength());
    }

    @Override
    protected void setRowInDecoder(Result result,EntryDecoder cachedDecoder){
        assert result!=null && result.size()>0: "Programmer error: Should not be called with an empty result!";
        Cell cell=CellUtils.matchDataColumn(result.rawCells());
        if(cell!=null)
            cachedDecoder.set(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
    }
}
