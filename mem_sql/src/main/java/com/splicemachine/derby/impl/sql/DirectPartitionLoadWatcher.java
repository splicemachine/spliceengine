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

package com.splicemachine.derby.impl.sql;

import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.storage.MPartitionLoad;
import com.splicemachine.storage.PartitionLoad;

import java.util.Collection;
import java.util.Collections;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class DirectPartitionLoadWatcher implements PartitionLoadWatcher{

    @Override
    public void startWatching(){

    }

    @Override
    public void stopWatching(){

    }

    @Override
    public Collection<PartitionLoad> tableLoad(String tableName, boolean refresh){
        return Collections.<PartitionLoad>singletonList(new MPartitionLoad(tableName));
    }
}
