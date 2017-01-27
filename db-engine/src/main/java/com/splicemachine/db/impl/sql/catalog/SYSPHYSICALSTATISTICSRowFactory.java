/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.*;
import java.sql.Types;

/**
 * @author Scott Fines
 *         Date: 2/25/15
 */
public class SYSPHYSICALSTATISTICSRowFactory extends CatalogRowFactory {
    public static final String TABLENAME_STRING = "SYSPHYSICALSTATS";
    private static final int COLUMN_COUNT = 4;
    private static final int HOSTNAME           = 1;
    private static final int NUMCPUS            = 2;
    private static final int MAX_HEAP           = 3;
    private static final int NETWORK_SIZE       = 4;

    private String[] uuids = {
            "08264013-014b-c29c-947b-000003009390",
            "08264013-014b-c29c-947b-000003009390"
    };
    public SYSPHYSICALSTATISTICSRowFactory(UUIDFactory uuidFactory, ExecutionFactory exFactory, DataValueFactory dvf) {
        super(uuidFactory,exFactory,dvf);
        initInfo(COLUMN_COUNT,TABLENAME_STRING,null,null,uuids);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {
        String hostName = null;
        int numCpus = 0;
        long maxHeap = 0;
        int numIpc = 0;

        if(td!=null){
            PhysicalStatsDescriptor psd = (PhysicalStatsDescriptor)td;
            hostName = psd.getHostName();
            numCpus = psd.getNumCores();
            maxHeap = psd.getHeapSize();
            numIpc = psd.getNumIpcThreads();
        }

        ExecRow row = getExecutionFactory().getValueRow(COLUMN_COUNT);
        row.setColumn(HOSTNAME,new SQLVarchar(hostName));
        row.setColumn(NUMCPUS,new SQLInteger(numCpus));
        row.setColumn(MAX_HEAP,new SQLLongint(maxHeap));
        row.setColumn(NETWORK_SIZE,new SQLInteger(numIpc));
        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row, TupleDescriptor parentTuple, DataDictionary dataDictionary) throws StandardException {

        DataValueDescriptor col = row.getColumn(HOSTNAME);
        String hostName = col.getString();
        col = row.getColumn(NUMCPUS);
        int numCores = col.getInt();
        col = row.getColumn(MAX_HEAP);
        long heapSize = col.getLong();
        col = row.getColumn(NETWORK_SIZE);
        int numIpcThreads = col.getInt();

        return new PhysicalStatsDescriptor(
                hostName,
                numCores,
                heapSize,
                numIpcThreads);

    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[] {
                SystemColumnImpl.getColumn("HOSTNAME", Types.VARCHAR, false),
                SystemColumnImpl.getColumn("NUM_CPUS", Types.INTEGER,true),
                SystemColumnImpl.getColumn("MAX_HEAP", Types.BIGINT,true),
                SystemColumnImpl.getColumn("NETWORK_CONNS", Types.INTEGER,true)
        };
    }
}
