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

package com.splicemachine.concurrent;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Scott Fines
 *         Date: 9/4/15
 */
public class ReentrantLockFactory implements LockFactory{
    private static final ReentrantLockFactory INSTANCE = new ReentrantLockFactory(false);
    private static final ReentrantLockFactory FAIR_INSTANCE = new ReentrantLockFactory(true);

    private final boolean fair;

    public ReentrantLockFactory(boolean fair){
        this.fair=fair;
    }

    @Override
    public Lock newLock(){
        return new ReentrantLock(fair);
    }

    public static LockFactory instance(){ return INSTANCE;}

    public static LockFactory fairInstance(){ return FAIR_INSTANCE;}

}
