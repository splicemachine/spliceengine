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

import com.splicemachine.derby.iapi.sql.olap.AbstractOlapResult;
import com.splicemachine.derby.stream.utils.ExternalTableUtils;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Created by jfilali on 1/12/17.
 * PlaceHolder for the schema information provided by Spark.
 */
public class GetSchemaExternalResult extends AbstractOlapResult {
    private StructType partition_schema;
    private StructType schema;

    public GetSchemaExternalResult(StructType schema) {
        this.schema = schema;
    }

    public GetSchemaExternalResult(StructType partition_schema, StructType schema) {
        this.partition_schema = partition_schema;
        this.schema = schema;
    }

    public StructType getSchema(){
        return schema;
    }
    public StructType getPartitionSchema(){
        return partition_schema;
    }

    @Override
    public boolean isSuccess(){
        return true;
    }

    public String getSuggestedSchema()
    {
        return ExternalTableUtils.getSuggestedSchema(schema, partition_schema);
    }

    public StructType getFullSchema()
    {
        if( partition_schema == null ) {
            return schema;
        }
        else
        {
            // add directory-partitioning to the end just like spark does
            StructType full_schema = new StructType(schema.fields());
            for (StructField f : partition_schema.fields()) {
                full_schema = full_schema.add(f);
            }
            return full_schema;
        }
    }
}
