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
 */
package com.splicemachine.storage.util;

import com.splicemachine.storage.Partition;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;
import java.io.IOException;
import static org.mockito.Mockito.*;

/**
 * Created by jleach on 8/1/16.
 */
public class PartitionUtilsTest {

    @Test (expected = IllegalStateException.class)
    public void testGetPartitionsInRange() throws IOException {
        Partition partitionInfo=mock(Partition.class);
        Scan scanInfo=mock(Scan.class);
        when(scanInfo.getStartRow()).thenReturn(HConstants.EMPTY_BYTE_ARRAY);
        when(scanInfo.getStopRow()).thenReturn(HConstants.EMPTY_BYTE_ARRAY);
        PartitionUtils.getPartitionsInRange(partitionInfo, scanInfo, true);
    }

}
