package com.splicemachine.hbase;

import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;

import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class WriteFailedException extends RetriesExhaustedWithDetailsException{
    private List<Row> failedRows;

    public WriteFailedException(List<Throwable> exceptions, List<Row> actions, List<String> hostnameAndPort) {
        super(exceptions, actions, hostnameAndPort);
        this.failedRows = actions;
    }

    public List<Row> getFailedRows() {
        return failedRows;
    }
}
