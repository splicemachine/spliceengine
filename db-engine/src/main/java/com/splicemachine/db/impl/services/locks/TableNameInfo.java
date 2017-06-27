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

package com.splicemachine.db.impl.services.locks;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;

import com.splicemachine.db.iapi.store.access.TransactionController;

import java.util.Hashtable;
import java.util.Map;

public class TableNameInfo {

	// things to look up table name etc
	private DataDictionary dd;
	private Map<Long,ConglomerateDescriptor> ddCache;			// conglomId -> conglomerateDescriptor
	private Map<UUID,TableDescriptor> tdCache;			// tableID UUID -> table descriptor
	private Hashtable tableCache;		// conglomId -> table descriptor
	private Hashtable indexCache;		// conglomId -> indexname

	public TableNameInfo(LanguageConnectionContext lcc, boolean andIndex)
		throws StandardException {

		tableCache = new Hashtable(31);
		if (andIndex)
			indexCache = new Hashtable(13);

		TransactionController tc = lcc.getTransactionExecute();

		dd = lcc.getDataDictionary();
		ddCache = dd.hashAllConglomerateDescriptorsByNumber(tc);
		tdCache = dd.hashAllTableDescriptorsByTableId(tc);
	}


	public String getTableName(Long conglomId) {
		if (conglomId == null)
			return "?";

		// see if we have already seen this conglomerate
		TableDescriptor td = (TableDescriptor) tableCache.get(conglomId);
		if (td == null)
		{
			// first time we see this conglomerate, get it from the
			// ddCache 
			ConglomerateDescriptor cd = ddCache.get(conglomId);

            if (cd != null)
            {
                // conglomerateDescriptor is not null, this table is known
                // to the data dictionary

                td =tdCache.get(cd.getTableID());
            }

			if ((cd == null) || (td == null))
			{
				String name;

				// this table is not know to the data dictionary.  This
				// can be caused by one of two reasons:  
				// 1. the table has just been dropped
				// 2. the table is an internal one that lives below
				// 		the data dictionary
				if (conglomId> 20) {
					// table probably dropped!  
					name = "*** TRANSIENT_" + conglomId;
				}
				else
				{
					// I am hoping here that we won't create more than
					// 20 tables before starting the data dictionary!

					// one of the internal tables -- HACK!!
					switch (conglomId.intValue())
					{
					case 0: 
						name = "*** INVALID CONGLOMERATE ***";
						break;

					case 1: 
						name = "ConglomerateDirectory";
						break;

					case 2: 
						name = "PropertyConglomerate";
						break;

					default:
						name = "*** INTERNAL TABLE " + conglomId;
						break;
					}
				}

				return name;
			}

            tableCache.put(conglomId, td);

			if ((indexCache != null) && cd.isIndex())
				indexCache.put(conglomId, cd.getConglomerateName());
		}

		return td.getName();
	}

	public String getTableType(Long conglomId) {
		if (conglomId == null)
			return "?";

		String type;

		TableDescriptor td = (TableDescriptor) tableCache.get(conglomId);
		if (td != null)
		{
			switch(td.getTableType())
			{
			case TableDescriptor.BASE_TABLE_TYPE:
				type = "T";
				break;

			case TableDescriptor.SYSTEM_TABLE_TYPE:
				type = "S";
				break;

			default: 
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT("Illegal table type " +
						  td.getName() + " " + td.getTableType());
				type = "?";
				break;
			}
		} else if (conglomId > 20)
		{
			type = "T";
		} else {
			type = "S";
		}

		return type;
	}

	public String getIndexName(Long conglomId) {
		if (conglomId == null)
			return "?";
		return (String) indexCache.get(conglomId);
	}
}
