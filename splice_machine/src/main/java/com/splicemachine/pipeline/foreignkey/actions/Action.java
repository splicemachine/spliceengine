package com.splicemachine.pipeline.foreignkey.actions;

import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.writehandler.WriteHandler;

import java.util.Objects;

public abstract class Action implements WriteHandler  {

    protected final Long childBaseTableConglomId;
    protected final Long backingIndexConglomId;
    protected boolean failed;
    protected WriteResult writeResult;

    protected Action(Long childBaseTableConglomId, Long backingIndexConglomId) {
        this.childBaseTableConglomId = childBaseTableConglomId;
        this.backingIndexConglomId = backingIndexConglomId;
        this.failed = false;
        this.writeResult = null;
    }

    public boolean hasFailed() {
        return failed;
    }

    public WriteResult getWriteResult() {
        assert failed;
        return writeResult;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Action that = (Action) o;
        return Objects.equals(childBaseTableConglomId, that.childBaseTableConglomId) &&
                Objects.equals(backingIndexConglomId, that.backingIndexConglomId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(childBaseTableConglomId, backingIndexConglomId);
    }
}
