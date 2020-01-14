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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.dictionary.CatalogRowFactory;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;

/**
* A poor mans structure used in DataDictionaryImpl.java.
* Used to save information about system indexes.
*
*/
class IndexInfoImpl
{
	private IndexRowGenerator	irg;

	private long				conglomerateNumber;
    
    private final CatalogRowFactory crf;
    private final int indexNumber;

	/**
	 * Constructor
	 *
	 * @param indexNumber			(0-based) number of index within catalog's indexes
	 * @param crf					CatalogRowFactory for the catalog
	 */
	IndexInfoImpl(int indexNumber, CatalogRowFactory crf)
	{
        this.crf = crf;
        this.indexNumber = indexNumber;
		this.conglomerateNumber = -1;
	}

    /**
	 * Get the conglomerate number for the index.
	 *
	 * @return long	The conglomerate number for the index.
	 */
	long getConglomerateNumber()
	{
		return conglomerateNumber;
	}

	/**
	 * Set the conglomerate number for the index.
	 *
	 * @param conglomerateNumber	The conglomerateNumber for the index.
	 */
	void setConglomerateNumber(long conglomerateNumber)
	{
		this.conglomerateNumber = conglomerateNumber;
	}

	/**
	 * Get the index name for the index.
	 *
	 * @return String	The index name for the index.
	 */
	String getIndexName()
	{
		return crf.getIndexName(indexNumber);
	}

	/**
	 * Get the column count for the index.
	 *
	 * @return int	The column count for the index.
	 */
	int getColumnCount()
	{
		return crf.getIndexColumnCount(indexNumber);
	}

	/**
	 * Get the IndexRowGenerator for this index.
	 *
	 * @return IndexRowGenerator	The IRG for this index.
	 */
	IndexRowGenerator getIndexRowGenerator()
	{
		return irg;
	}

	/**
	 * Set the IndexRowGenerator for this index.
	 *
	 * @param irg			The IndexRowGenerator for this index.
	 */
	void setIndexRowGenerator(IndexRowGenerator irg)
	{
		this.irg = irg;
	}

	/**
	 * Get the base column position for a column within a catalog
	 * given the (0-based) column number for the column within the index.
	 *
	 * @param colNumber		The column number within the index
	 *
	 * @return int		The base column position for the column.
	 */
	int getBaseColumnPosition(int colNumber)
	{
		return crf.getIndexColumnPositions(indexNumber)[colNumber];
	}

	/**
	 * Return whether or not this index is declared unique
	 *
	 * @return boolean		Whether or not this index is declared unique
	 */
	boolean isIndexUnique()
	{
		return crf.isIndexUnique(indexNumber);
	}

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "name: " + this.getIndexName() +
				"\n\tconglomerateNumber: " + conglomerateNumber +
				"\n\tindexNumber: " + indexNumber +
				"\n";
		}
		else
		{
			return "";
		}
	}
}
