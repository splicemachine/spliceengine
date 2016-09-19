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

package com.splicemachine.derby.hbase;

import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public interface SplitObserver{

    void preSplit(ObserverContext<RegionCoprocessorEnvironment> c,
                  @Nullable byte[] splitRow) throws IOException;

    void postCompleteSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException;

}
