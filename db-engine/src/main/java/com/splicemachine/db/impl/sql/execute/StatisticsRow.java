package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.stats.ItemStatistics;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
/**
 * Created by jleach on 8/1/16.
 */
public class StatisticsRow extends ValueRow {
    private ItemStatistics[] statistics;

    public StatisticsRow(ExecRow execRow) throws StandardException {
        assert execRow!=null:"ExecRow passed in is null";
        this.setRowArray(execRow.getRowArray());
        statistics = new ItemStatistics[execRow.nColumns()];
        for (int i = 0; i< execRow.nColumns(); i++) {
            DataValueDescriptor dvd = execRow.getColumn(i+1);
            statistics[i] = new ColumnStatisticsImpl(dvd);
        }
    }


    /**
     *
     * Sets the statistical values as well.
     *
     * @param position
     * @param col
     */
    @Override
    public void setColumn(int position, DataValueDescriptor col) {
        statistics[position-1].update(col);
    }

    public void setExecRow(ExecRow execRow) throws StandardException {
        for (int i = 1; i<= execRow.nColumns(); i++) {
            setColumn(i,execRow.getColumn(i));
        }
    }

    public ItemStatistics[] getItemStatistics() {
        return statistics;
    }
}