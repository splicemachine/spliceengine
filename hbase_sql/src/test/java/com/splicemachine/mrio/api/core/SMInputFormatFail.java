package com.splicemachine.mrio.api.core;

import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.List;

/**
 * @author Igor Praznik
 *
 * Test extendtion of SMInputFormat to simulate gaps in split rows for refresh.
 */
public class SMInputFormatFail extends SMInputFormat {

    @Override
    protected boolean isRefreshNeeded(List<InputSplit> lss) {
        corruptStartRow(lss);
        return super.isRefreshNeeded(lss);
    }

    private void corruptStartRow(List<InputSplit> lss) {
        int corruptedItemIndex = lss.size() / 2;

        SMSplit smSplitToCorrupt = ((SMSplit)lss.get(corruptedItemIndex));

        TableSplit origSplit = smSplitToCorrupt.split;

        smSplitToCorrupt.split = new TableSplit(origSplit.getTable(), origSplit.getStartRow().clone(),
                origSplit.getEndRow().clone(), origSplit.getRegionLocation());

        byte[] corruptedRow = smSplitToCorrupt.split.getStartRow();

        corruptedRow[0] = (byte) (corruptedRow[0] + 1);
    }
}
