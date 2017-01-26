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

package com.splicemachine.test_tools;

/**
 * @author Scott Fines
 *         Date: 8/28/15
 */
public abstract class CountGeneratedRowCreator implements RowCreator{
    private final int batchSize;
    private final int maxCount;
    protected int position = 0;

    public CountGeneratedRowCreator(int maxCount){
        this(maxCount,1);
    }

    public CountGeneratedRowCreator(int maxCount,int batchSize){
        this.maxCount=maxCount;
        this.batchSize = batchSize;
    }

    @Override
    public boolean advanceRow(){
        return position++<=maxCount;
    }

    @Override public int batchSize(){ return batchSize; }

    @Override
    public void reset(){
       position = 0;
    }
}
