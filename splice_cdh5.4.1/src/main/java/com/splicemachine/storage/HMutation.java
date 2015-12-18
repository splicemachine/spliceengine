package com.splicemachine.storage;

import org.apache.hadoop.hbase.client.Mutation;

/**
 * @author Scott Fines
 *         Date: 12/17/15
 */
public interface HMutation extends DataMutation{

    Mutation unwrapHbaseMutation();
}
