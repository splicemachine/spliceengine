/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.AggregateAliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;

/**
 * Represents a system aggregate function in the data dictionary.
 * 
 * @author Jun Yuan
 */
public class Aggregate {

    private String name;
    private TypeDescriptor inType;
    private TypeDescriptor returnType;
    private String javaClassName;

    public Aggregate() {}

    public Aggregate(String name, TypeDescriptor inType, TypeDescriptor returnType, String javaClassName) {
        this.name = name;
        this.inType = inType;
        this.returnType = returnType;
        this.javaClassName = javaClassName;
    }

    public void createSystemAggregate(DataDictionary dataDictionary, TransactionController tc)
		throws StandardException {
    	// By default, puts aggregate function into SYSCS_UTIL schema
    	// unless you invoke the method that takes schema UUID argument.
        UUID schemaId = dataDictionary.getSystemUtilSchemaDescriptor().getUUID();
        createSystemAggregate(dataDictionary, tc, schemaId);
    }

    public void createSystemAggregate(DataDictionary dataDictionary, TransactionController tc, UUID schemaId)
		throws StandardException {

        char aliasType = AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR;
        char namespace = AliasInfo.ALIAS_NAME_SPACE_AGGREGATE_AS_CHAR;

        AggregateAliasInfo aliasInfo = new AggregateAliasInfo(inType, returnType);

        UUID aggregate_uuid = dataDictionary.getUUIDFactory().createUUID();

        AliasDescriptor ads =
                new AliasDescriptor(
                        dataDictionary,
                        aggregate_uuid,
                        name,
                        schemaId,
                        javaClassName,
                        aliasType,
                        namespace,
                        false,
                        aliasInfo,
                        null);
        dataDictionary.addDescriptor(
                ads, null, DataDictionary.SYSALIASES_CATALOG_NUM, false, tc);
    }
}
