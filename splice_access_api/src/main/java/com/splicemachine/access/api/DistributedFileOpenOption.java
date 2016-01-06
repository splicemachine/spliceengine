package com.splicemachine.access.api;

import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;

/**
 *
 * @author Scott Fines
 *         Date: 1/8/16
 */
public class DistributedFileOpenOption implements OpenOption{
    private final int replication;
    private final StandardOpenOption standardOpenOption;

    public DistributedFileOpenOption(int replication,StandardOpenOption standardOpenOption){
        this.replication=replication;
        this.standardOpenOption = standardOpenOption;
    }

    public int getReplication(){
        return replication;
    }

    public StandardOpenOption standardOption(){
        return standardOpenOption;
    }
}
