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

/**
 * A Factory for constructing locks. This is useful when you want to replace different lock implementations
 * (i.e. for simulating lock timeout etc. without relying on the system clock).
 *
 * @author Scott Fines
 *         Date: 9/4/15
 */
public interface LockFactory{

    /**
     * @return a new lock from the factory
     */
    Lock newLock();
}
