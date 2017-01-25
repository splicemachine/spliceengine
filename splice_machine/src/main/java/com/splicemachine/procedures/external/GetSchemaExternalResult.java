package com.splicemachine.procedures.external;

import com.splicemachine.derby.iapi.sql.olap.AbstractOlapResult;
import org.apache.spark.sql.types.StructType;

/**
 * Created by jfilali on 1/12/17.
 * PlaceHolder for the schema information provided by Spark.
 */
public class GetSchemaExternalResult extends AbstractOlapResult {
    private StructType schema;

    public GetSchemaExternalResult(StructType schema) {
        this.schema = schema;
    }

    public  StructType getSchema(){
        return  schema;
    }

    @Override
    public boolean isSuccess(){
        return true;
    }
}
