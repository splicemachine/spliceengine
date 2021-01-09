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

package com.splicemachine.procedures.external;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.system.CsvOptions;
import org.apache.spark.sql.types.StructType;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.*;

/**
 * An Distributed job to get the schema of the external file
 */
public class DistributedGetSchemaExternalJob extends DistributedJob implements Externalizable {

    private String location;
    private String storedAs;
    private String jobGroup;
    private boolean mergeSchema;
    private CsvOptions csvOptions;
    private StructType nonPartitionColumns;
    private StructType partitionColumns;

    public DistributedGetSchemaExternalJob() {
    }

    DistributedGetSchemaExternalJob(String location,
                                           String jobGroup,
                                           String storedAs,
                                           boolean mergeSchema,
                                           CsvOptions csvOptions,
                                           StructType nonPartitionColumns,
                                           StructType partitionColumns) {
        this.storedAs = storedAs;
        this.location = location;
        this.jobGroup = jobGroup;
        this.mergeSchema = mergeSchema;
        this.csvOptions = csvOptions;
        this.nonPartitionColumns = nonPartitionColumns;
        this.partitionColumns = partitionColumns;
    }

    public static GetSchemaExternalResult execute(String location,
                 String jobGroup,
                 String storedAs,
                 boolean mergeSchema,
                 CsvOptions csvOptions,
                 StructType nonPartitionColumns,
                 StructType partitionColumns) throws IOException {
        Future<GetSchemaExternalResult> futureResult = EngineDriver.driver().getOlapClient().
                submit(new DistributedGetSchemaExternalJob(location, jobGroup, storedAs, mergeSchema, csvOptions,
                        nonPartitionColumns, partitionColumns));
        GetSchemaExternalResult result = null;
        SConfiguration config = EngineDriver.driver().getConfiguration();

        while (result == null) {
            try {
                result = futureResult.get(config.getOlapClientTickTime(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                //we were interrupted processing, so we're shutting down. Nothing to be done, just die gracefully
                Thread.currentThread().interrupt();
                throw new IOException(e);
            } catch (ExecutionException e) {
                throw Exceptions.rawIOException(e.getCause());
            } catch (TimeoutException e) {
                /*
                 * A TimeoutException just means that tickTime expired. That's okay, we just stick our
                 * head up and make sure that the client is still operating
                 */
            }
        }
        return result;
    }

    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus, Clock clock, long clientTimeoutCheckIntervalMs) {
        return new GetSchemaExternalJob(this, jobStatus);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeBoolean(storedAs!=null);
        if (storedAs!=null)
            out.writeUTF(storedAs);

        out.writeBoolean(location!=null);
        if (location!=null)
            out.writeUTF(location);

        out.writeUTF(jobGroup);
        out.writeBoolean(mergeSchema);
        csvOptions.writeExternal(out);
        out.writeUTF(nonPartitionColumns == null ? "" : nonPartitionColumns.json());
        out.writeUTF(partitionColumns == null ? "" : partitionColumns.json());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        storedAs    = in.readBoolean()?in.readUTF():null;
        location    = in.readBoolean()?in.readUTF():null;
        jobGroup    = in.readUTF();
        mergeSchema = in.readBoolean();
        csvOptions  = new CsvOptions(in);
        String s = in.readUTF();
        nonPartitionColumns = s.isEmpty() ? null : (StructType)StructType.fromJson(s);
        s = in.readUTF();
        partitionColumns = s.isEmpty() ? null : (StructType)StructType.fromJson(s);
    }


    public String getLocation() {
        return location;
    }

    public String getJobGroup() {
        return jobGroup;
    }

    public String getStoredAs(){ return storedAs; }

    public boolean mergeSchema() {return mergeSchema;}

    public CsvOptions getCsvOptions() { return csvOptions; }

    public StructType getNonPartitionColumns() {
        return nonPartitionColumns;
    }
    public StructType getPartitionColumns() {
        return partitionColumns;
    }
}
