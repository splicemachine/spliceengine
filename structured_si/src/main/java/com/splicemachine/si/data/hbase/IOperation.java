package com.splicemachine.si.data.hbase;

import org.apache.hadoop.hbase.client.OperationWithAttributes;

public interface IOperation {
    OperationWithAttributes getOperation();
}
