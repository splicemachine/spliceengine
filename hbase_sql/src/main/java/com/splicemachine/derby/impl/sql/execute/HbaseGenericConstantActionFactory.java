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

package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;

/**
 * @author Scott Fines
 *         Date: 1/27/16
 */
public class HbaseGenericConstantActionFactory extends SpliceGenericConstantActionFactory{
    @Override
    public ConstantAction getDropIndexConstantAction(String fullIndexName,String indexName,String tableName,String schemaName,UUID tableId,long tableConglomerateId){
        return new HBaseDropIndexConstantOperation(fullIndexName,indexName,tableName,schemaName,tableId,tableConglomerateId);
    }
}
