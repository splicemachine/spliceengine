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

package com.splicemachine.derby.utils;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.si.impl.driver.SIDriver;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/27/16
 */
public class ZkSequencer implements Sequencer{
    private final String path;

    public ZkSequencer(){
        path =SIDriver.driver().getConfiguration().getSpliceRootPath()+HConfiguration.CONGLOMERATE_SCHEMA_PATH+"/__CONGLOM_SEQUENCE";
    }

    @Override
    public long next() throws IOException{
        return ZkUtils.nextSequenceId(path);
    }

    @Override
    public void setPosition(long sequence) throws IOException{
        ZkUtils.setSequenceId(path,sequence);
    }
}
