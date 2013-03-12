package com.splicemachine.si2.data.hbase;

import org.apache.hadoop.hbase.client.OperationWithAttributes;

public interface IOperation {
    OperationWithAttributes getOperation();
}
