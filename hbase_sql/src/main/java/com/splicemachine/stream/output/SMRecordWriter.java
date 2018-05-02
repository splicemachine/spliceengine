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

package com.splicemachine.stream.output;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.spark.SparkOperationContext;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.stream.SecurityUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import scala.util.Either;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Created by jleach on 5/18/15.
 */
public class SMRecordWriter extends RecordWriter<RowLocation,Either<Exception, ExecRow>> {
    private static Logger LOG = Logger.getLogger(SMRecordWriter.class);
    private boolean initialized = false;
    private TableWriter tableWriter;
    private OutputCommitter outputCommitter;
    private boolean failure = false;
    private ActivationHolder activationHolder;

    private UserGroupInformation ugi;

    public SMRecordWriter(TableWriter tableWriter, OutputCommitter outputCommitter) throws StandardException{
        try {
            SpliceLogUtils.trace(LOG, "init");
            this.tableWriter = tableWriter;
            this.outputCommitter = outputCommitter;
            SparkOperationContext context = (SparkOperationContext)tableWriter.getOperationContext();
            if (context != null) {
                activationHolder = context.getActivationHolder();
                activationHolder.reinitialize(tableWriter.getTxn());
                SConfiguration conf = SIDriver.driver().getConfiguration();
                String authentication = conf.getAuthentication();
                if (authentication.compareToIgnoreCase(Property.AUTHENTICATION_PROVIDER_LDAP) == 0) {
                    SpliceBaseOperation op = (SpliceBaseOperation)context.getOperation();
                    String user = op.getUser();
                    String password = op.getPassword();
                    ugi = SecurityUtils.loginAndReturnUGI("SMRecordWriter", user, password);
                }
            }
        }
        catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void write(RowLocation rowLocation, Either<Exception, ExecRow> value) throws IOException, InterruptedException {
        if (ugi != null) {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {
                    doWrite(rowLocation, value);
                    return null;
                }
            });
        }
        else {
            doWrite(rowLocation, value);
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        SpliceLogUtils.trace(LOG,"closing %s",taskAttemptContext);
        try {
            if (initialized) {
                tableWriter.close();
            }
        } catch (Exception e) {
            failure = true;
            throw new IOException(e);
        } finally {
            if (activationHolder != null) {
                activationHolder.close();
            }
            if (failure)
                outputCommitter.abortTask(taskAttemptContext);
        }
    }

    private void doWrite(RowLocation rowLocation, Either<Exception, ExecRow> value) throws IOException, InterruptedException {
        if (value.isLeft()) {
            Exception e = value.left().get();
            // failure
            failure = true;
            SpliceLogUtils.error(LOG,"Error Reading",e);
            throw new IOException(e);
        }
        assert value.isRight();
        ExecRow execRow = value.right().get();
        try {
            if (!initialized) {
                initialized = true;
                tableWriter.open();
            }
            tableWriter.write(execRow);
        } catch (Exception se) {
            SpliceLogUtils.error(LOG,"Error Writing",se);
            failure = true;
            throw new IOException(se);
        }
    }
}
