/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.si.data.hbase.coprocessor;

import com.google.protobuf.Service;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 6/19/14
 */
public class TxnLifecycleEndpoint extends BaseTxnLifecycleEndpoint implements Coprocessor, CoprocessorService {

    private static final Logger LOG=Logger.getLogger(TxnLifecycleEndpoint.class);

    @Override
    public void start(CoprocessorEnvironment env) throws IOException{
        startAction(env);
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        stopAction(env);
    }


    @Override
    public Service getService(){
        SpliceLogUtils.trace(LOG,"Getting the TxnLifecycle Service");
        return this;
    }

}
