package com.splicemachine.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.ExploringCompactionPolicy;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Simple compaction policy extending HBase's default in order to create our own CompactionRequest
 */
public class SpliceDefaultCompactionPolicy extends ExploringCompactionPolicy {
    /**
     * Constructor for ExploringCompactionPolicy.
     *
     * @param conf            The configuration object
     * @param storeConfigInfo An object to provide info about the store.
     */
    public SpliceDefaultCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
        super(conf, storeConfigInfo);
    }

    @Override
    public CompactionRequest selectCompaction(Collection<StoreFile> candidateFiles, List<StoreFile> filesCompacting, boolean isUserCompaction, boolean mayUseOffPeak, boolean forceMajor) throws IOException {
        CompactionRequest cr = super.selectCompaction(candidateFiles, filesCompacting, isUserCompaction, mayUseOffPeak, forceMajor);
        SpliceCompactionRequest scr = new SpliceCompactionRequest();
        scr.combineWith(cr);
        return scr;
    }
}
