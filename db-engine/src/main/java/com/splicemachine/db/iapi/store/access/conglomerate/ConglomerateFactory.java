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

package com.splicemachine.db.iapi.store.access.conglomerate;

import java.util.Properties;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**

  The factory interface for all conglomerate access methods.

**/

public interface ConglomerateFactory extends MethodFactory
{

    static final int    HEAP_FACTORY_ID     = 0x00;
    static final int    BTREE_FACTORY_ID    = 0x01;


    /**
     * Return the conglomerate factory id.
     * <p>
     * Return a number in the range of 0-15 which identifies this factory.
     * Code which names conglomerates depends on this range currently, but
     * could be easily changed to handle larger ranges.   One hex digit seemed
     * reasonable for the number of conglomerate types currently implemented
     * (heap, btree) and those that might be implmented in the future: gist, 
     * gist btree, gist rtree, hash, others? ).
     * <p>
     *
	 * @return an unique identifier used to the factory into the conglomid.
     *
     **/
    int getConglomerateFactoryId();

	/**
	Create the conglomerate and return a conglomerate object
	for it.  It is expected that the caller of this method will place the
    the resulting object in the conglomerate directory.

    @param xact_mgr             transaction to perform the create in.
    @param input_containerid    containerid to assign the container, or
                                ContainerHandle.DEFAULT_ASSIGN_ID if you want
                                raw store to assign an id.
    @param template             Template of row in the conglomerate.
	@param columnOrder          columns sort order for Index creation
    @param collationIds         collation ids of columns in the conglomerate.
    @param properties           Properties associated with the conglomerate.

 	@exception StandardException if the conglomerate could not be
	opened for some reason, or if an error occurred in one of
	the lower level modules.
	**/
	Conglomerate createConglomerate(
	boolean					isExternal,
    TransactionManager      xact_mgr,
    long                    input_containerid,
    DataValueDescriptor[]   template,
	ColumnOrdering[]		columnOrder,
    int[]                   collationIds,
    Properties              properties,
	int						temporaryFlag)
            throws StandardException;
    /**
     * Return Conglomerate object for conglomerate with container_key.
     * <p>
     * Return the Conglomerate Object.  This is implementation specific.
     * Examples of what will be done is using the key to find the file where
     * the conglomerate is located, and then executing implementation specific
     * code to instantiate an object from reading a "special" row from a
     * known location in the file.  In the btree case the btree conglomerate
     * is stored as a column in the control row on the root page.
     * <p>
     * This operation is costly so it is likely an implementation using this
     * will cache the conglomerate row in memory so that subsequent accesses
     * need not perform this operation.
     *
     * @param xact_mgr      transaction to perform the create in.
     * @param containerID The unique id of the existing conglomerate.
     *
	 * @return An instance of the conglomerate.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    Conglomerate readConglomerate(
    TransactionManager      xact_mgr,
    long            containerID)
		throws StandardException;
}
