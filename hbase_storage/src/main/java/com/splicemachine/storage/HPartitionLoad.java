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

package com.splicemachine.storage;

/**
 * @author Scott Fines
 *         Date: 1/7/16
 */
public class HPartitionLoad implements PartitionLoad{
    private final long storefileSize;
    private final long memStoreSize;
    private final String name;

    public HPartitionLoad(String name,long storefileSize, long memStoreSize){
        this.storefileSize=storefileSize;
        this.memStoreSize=memStoreSize;
        this.name = name;
    }

    @Override
    public String getPartitionName(){
        return name;
    }

    @Override
    public long getStorefileSize(){
        return storefileSize;
    }

    @Override
    public long getMemStoreSize(){
        return memStoreSize;
    }

    @Override
    public long getStorefileIndexSize(){
        return 0;
    }
}
