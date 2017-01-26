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

package com.splicemachine.storage;

import com.splicemachine.access.api.ServerControl;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class MServerControl implements ServerControl{
    public static final ServerControl INSTANCE = new MServerControl();

    private MServerControl(){}
    @Override public void startOperation() throws IOException{ }
    @Override public void stopOperation() throws IOException{ }
    @Override public void ensureNetworkOpen() throws IOException{ }

    @Override
    public boolean isAvailable(){
        return true;
    }
}
