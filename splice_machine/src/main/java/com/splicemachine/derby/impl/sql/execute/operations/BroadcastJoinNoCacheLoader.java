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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.stream.Stream;

import java.util.concurrent.Callable;

/**
 * Created by yxia on 7/22/20.
 */
public class BroadcastJoinNoCacheLoader implements Callable<JoinTable.Factory> {
    private final BroadcastJoinCache.JoinTableLoader tableLoader = ValueRowMapTableLoader.INSTANCE;
    private final int[] innerHashKeys;
    private final int[] outerHashKeys;
    private final ExecRow outerTemplateRow;
    private final Callable<Stream<ExecRow>> streamLoader;

    private final Long operationId;

    public BroadcastJoinNoCacheLoader(Long operationId,
                  int[] innerHashKeys,
                  int[] outerHashKeys,
                  ExecRow outerTemplateRow,
                  Callable<Stream<ExecRow>> streamLoader){
        this.operationId=operationId;
        this.innerHashKeys=innerHashKeys;
        this.outerHashKeys=outerHashKeys;
        this.outerTemplateRow=outerTemplateRow;
        this.streamLoader=streamLoader;
    }

    @Override
    public JoinTable.Factory call() throws Exception {
        return tableLoader.load(streamLoader,innerHashKeys,outerHashKeys,outerTemplateRow);
    }
}