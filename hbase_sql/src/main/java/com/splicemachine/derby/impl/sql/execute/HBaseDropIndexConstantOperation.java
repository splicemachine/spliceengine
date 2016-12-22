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

/**
 * @author Scott Fines
 *         Date: 1/13/16
 */
public class HBaseDropIndexConstantOperation extends AbstractDropIndexConstantOperation{
    /**
     * Make the ConstantAction for a DROP INDEX statement.
     *
     * @param tableId             UUID for table
     * @param tableConglomerateId heap Record Id for table
     * @param    fullIndexName        Fully qualified index name
     * @param    indexName            Index name.
     * @param    tableName            The table name
     * @param    schemaName            Schema that index lives in.
     */
    public HBaseDropIndexConstantOperation(String fullIndexName,
                                           String indexName,
                                           String tableName,
                                           String schemaName,
                                           UUID tableId,
                                           long tableConglomerateId){
        super(fullIndexName,indexName,tableName,schemaName,tableId,tableConglomerateId);
    }

}
