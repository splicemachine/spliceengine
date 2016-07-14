/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
