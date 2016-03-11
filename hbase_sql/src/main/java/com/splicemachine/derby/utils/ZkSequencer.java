package com.splicemachine.derby.utils;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.si.impl.driver.SIDriver;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/27/16
 */
public class ZkSequencer implements Sequencer{
    private final String path;

    public ZkSequencer(){
        path =SIDriver.driver().getConfiguration().getSpliceRootPath()+HConfiguration.CONGLOMERATE_SCHEMA_PATH+"/__CONGLOM_SEQUENCE";
    }

    @Override
    public long next() throws IOException{
        return ZkUtils.nextSequenceId(path);
    }

    @Override
    public void setPosition(long sequence) throws IOException{
        ZkUtils.setSequenceId(path,sequence);
    }
}
