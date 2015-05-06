package com.splicemachine.derby.stream.temporary.update;

import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.si.api.TxnView;

/**
 * Created by jleach on 5/5/15.
 */
public class UpdateTableWriterBuilder {
    protected long heapConglom;
    protected int[] formatIds;
    protected int[] columnOrdering;
    protected int[] pkCols;
    protected FormatableBitSet pkColumns;
    protected FormatableBitSet heapList;
    protected String tableVersion;
    protected TxnView txn;
    protected ExecRow execRowDefinition;

    public UpdateTableWriterBuilder tableVersion(String tableVersion) {
        assert tableVersion != null :"Table Version Cannot Be null!";
        this.tableVersion = tableVersion;
        return this;
    }

    public UpdateTableWriterBuilder txn(TxnView txn) {
        assert txn != null :"Txn Cannot Be null!";
        this.txn = txn;
        return this;
    }

    public UpdateTableWriterBuilder execRowDefinition(ExecRow execRowDefinition) {
        assert execRowDefinition != null :"ExecRowDefinition Cannot Be null!";
        this.execRowDefinition = execRowDefinition;
        return this;
    }

    public UpdateTableWriterBuilder heapConglom(long heapConglom) {
        assert heapConglom !=-1 :"Only Set Heap Congloms allowed!";
        this.heapConglom = heapConglom;
        return this;
    }

    public UpdateTableWriterBuilder formatIds(int[] formatIds) {
        assert formatIds != null :"Format ids cannot be null";
        this.formatIds = formatIds;
        return this;
    }

    public UpdateTableWriterBuilder columnOrdering(int[] columnOrdering) {
        assert columnOrdering != null :"Column Ordering cannot be null";
        this.columnOrdering = columnOrdering;
        return this;
    }

    public UpdateTableWriterBuilder pkCols(int[] pkCols) {
        assert pkCols != null :"Primary Key Columns cannot be null";
        this.pkCols = pkCols;
        return this;
    }

    public UpdateTableWriterBuilder pkColumns(FormatableBitSet pkColumns) {
        assert pkColumns != null :"PK Columns cannot be null";
        this.pkColumns = pkColumns;
        return this;
    }

    public UpdateTableWriterBuilder heapList(FormatableBitSet heapList) {
        assert heapList != null :"heapList cannot be null";
        this.heapList = heapList;
        return this;
    }

}
