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

package com.splicemachine.si.impl.readresolve;

import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.utils.ByteSlice;

/**
 * @author Scott Fines
 *         Date: 6/26/14
 */
public class NoOpReadResolver implements ReadResolver {
		public static final ReadResolver INSTANCE = new NoOpReadResolver();
    @Override public void resolve(ByteSlice rowKey, long txnId) {  }

    @Override
    public boolean enabled() {
        return false;
    }
}
