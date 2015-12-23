package com.splicemachine.access.api;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public interface PartitionCreator{

    PartitionCreator withName(String name);

    PartitionCreator withCoprocessor(String coprocessor) throws IOException;

    void create() throws IOException;
}
