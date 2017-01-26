/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.pipeline.context;

import com.splicemachine.pipeline.api.PipelineMeter;


/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class NoOpPipelineMeter implements PipelineMeter{
    public static final PipelineMeter INSTANCE = new NoOpPipelineMeter();

    private NoOpPipelineMeter(){}

    @Override public void mark(int numSuccess,int numFailed){ }

    @Override public void rejected(int numRows){ }

    @Override public double throughput(){ return 0; }
    @Override public double fifteenMThroughput(){ return 0; }
    @Override public double fiveMThroughput(){ return 0; }
    @Override public double oneMThroughput(){ return 0; }
    @Override public long rejectedCount(){ return 0; }
}
