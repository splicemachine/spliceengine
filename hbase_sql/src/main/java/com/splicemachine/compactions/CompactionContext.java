/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *  
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *  
 */

package com.splicemachine.compactions;

import com.splicemachine.si.impl.server.Compactor;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/8/16
 */
public interface CompactionContext{
    long getSmallestReadPoint();

    List<String> getFilePaths();

    long fileSize();

    long getSelectionTime();

    boolean isMajor();

    void cancel();

    void complete();

    ProgressWatcher progressWatcher();

    boolean shouldCleanSeqId() throws IOException;

    int compactionKVMax();

    StoreFileWriter createTmpWriter() throws IOException;

    boolean isAllFiles();

    long maxSeqId();

    boolean retainDeleteMarkers();

    List<StoreFileScanner> createFileScanners() throws IOException;

    long earliestPutTs();

    void closeStoreFileReaders(boolean b) throws IOException;

    Compactor.Accumulator accumulator();

    interface ProgressWatcher{
        void incrementCompactedKVs();
        void incrementCompactedSize(long add);

        void markComplete();
    }
}
