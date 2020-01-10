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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.regionserver.HRegion;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author Scott Fines
 *         Date: 12/15/15
 */
public class HLock implements Lock{

    private final byte[] key;

    private HRegion.RowLock delegate;
    private HRegion region;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public HLock(HRegion region,byte[] key){
        this.key = key;
        this.region = region;
    }

    @Override public void lock(){
        throw new UnsupportedOperationException("Lock Called");
    }

    @Override
    public void lockInterruptibly() throws InterruptedException{
        lock();
    }

    /**
     *
     * Try Lock Implementation
     *
     * // HBase as part of its 1.1.x release modified the locks to
     * // throw IOException when it cannot be acquired vs. returning null
     *
     * @return
     */
    @Override
    public boolean tryLock(){
        try{
            delegate = region.getRowLock(key); // Null Lock Delegate means not run...
            return delegate!=null;
        }catch(IOException e){
            return false;
        }
    }

    /**
     *
     * The TimeUnit passed in is not used for HBase Row Locks
     *
     * @param time
     * @param unit
     * @return
     * @throws InterruptedException
     */
    @Override
    public boolean tryLock(long time,@Nonnull TimeUnit unit) throws InterruptedException{
        return tryLock();
    }

    @Override
    public void unlock(){
        if(delegate!=null) delegate.release();
    }

    @Override
    public @Nonnull Condition newCondition(){
        throw new UnsupportedOperationException("Cannot support conditions on an HLock");
    }
}
