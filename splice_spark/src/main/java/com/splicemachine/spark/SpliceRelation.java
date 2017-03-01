package com.splicemachine.spark;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StructType;

/**
 * Created by jleach on 2/27/17.
 */
public class SpliceRelation extends BaseRelation {

    @Override
    public SQLContext sqlContext() {
        return null;
    }

    @Override
    public StructType schema() {
        return null;
    }



}


