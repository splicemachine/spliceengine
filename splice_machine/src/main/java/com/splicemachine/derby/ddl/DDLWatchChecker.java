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

package com.splicemachine.derby.ddl;

import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.utils.Pair;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 9/7/15
 */
public interface DDLWatchChecker{

    boolean initialize(CommunicationListener listener) throws IOException;

    Collection<String> getCurrentChangeIds() throws IOException;

    DDLChange getChange(String changeId) throws IOException;

    void notifyProcessed(Collection<Pair<DDLChange,String>> processedChanges) throws IOException;

    void killDDLTransaction(String key);
}
