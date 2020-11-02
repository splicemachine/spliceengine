/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

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

    public WriteResult getFailedWriteResult() {
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
