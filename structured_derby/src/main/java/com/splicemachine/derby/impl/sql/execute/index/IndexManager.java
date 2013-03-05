package com.splicemachine.derby.impl.sql.execute.index;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public interface IndexManager extends Constraint{

    Put getIndexPut(Put baseTablePut) throws IOException;

    void updateIndex(Put indexPut,CoprocessorEnvironment coprocessorEnvironment) throws IOException;

    Delete getIndexDelete(Delete delete) throws IOException;

    void updateIndex(Delete indexDelete, CoprocessorEnvironment environment) throws IOException;
}
