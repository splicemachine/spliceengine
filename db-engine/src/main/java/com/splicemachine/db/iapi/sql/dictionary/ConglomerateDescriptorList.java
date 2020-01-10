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

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.catalog.UUID;

import java.util.Iterator;
import java.util.ArrayList;

public class ConglomerateDescriptorList extends ArrayList<ConglomerateDescriptor>
{

	/**
	 * Get a conglomerate descriptor by its number
	 *
	 * @param conglomerateNumber	The number of the conglomerate we're looking for
	 *
	 * @return	The ConglomerateDescriptor if found in this list,
	 *		null if not found.
	 */
	public ConglomerateDescriptor getConglomerateDescriptor(long conglomerateNumber)
	{
		ConglomerateDescriptor conglomerateDescriptor;
		ConglomerateDescriptor	returnValue = null;

		int size = size();
        for (ConglomerateDescriptor conglomerateDescriptor1 : this) {
            conglomerateDescriptor = (ConglomerateDescriptor) conglomerateDescriptor1;
            if (conglomerateNumber == conglomerateDescriptor.getConglomerateNumber()) {
                returnValue = conglomerateDescriptor;
                break;
            }
        }

		return returnValue;
	}

	/**
	 * Get an array of conglomerate descriptors with the given conglomerate
	 * number.  We get more than one descriptors if duplicate indexes share
	 * one conglomerate.
	 *
	 * @param conglomerateNumber	The number of the conglomerate
	 *
	 * @return	Array of ConglomerateDescriptors if found in this list,
	 *		size 0 array if not found.
	 */
	public ConglomerateDescriptor[] getConglomerateDescriptors(long conglomerateNumber)
	{
		ConglomerateDescriptor conglomerateDescriptor;

		int size = size(), j = 0;
		ConglomerateDescriptor[] draft = new ConglomerateDescriptor[size];

        for (ConglomerateDescriptor conglomerateDescriptor1 : this) {
            conglomerateDescriptor = (ConglomerateDescriptor) conglomerateDescriptor1;
            if (conglomerateNumber == conglomerateDescriptor.getConglomerateNumber())
                draft[j++] = conglomerateDescriptor;
        }

		if (j == size)
			return draft;
		ConglomerateDescriptor[] returnValue = new ConglomerateDescriptor[j];
        System.arraycopy(draft, 0, returnValue, 0, j);

		return returnValue;
	}


	/**
	 * Get a conglomerate descriptor by its Name
	 *
	 * @param conglomerateName	The Name of the conglomerate we're looking for
	 *
	 * @return	The ConglomerateDescriptor if found in this list,
	 *		null if not found.
	 */

	public ConglomerateDescriptor getConglomerateDescriptor(String conglomerateName)
	{
		ConglomerateDescriptor conglomerateDescriptor;
		ConglomerateDescriptor	returnValue = null;

		int size = size();
        for (ConglomerateDescriptor conglomerateDescriptor1 : this) {
            conglomerateDescriptor = (ConglomerateDescriptor) conglomerateDescriptor1;
            if (conglomerateName.equals(conglomerateDescriptor.getConglomerateName())) {
                returnValue = conglomerateDescriptor;
                break;
            }
        }

		return returnValue;
	}

	/**
	 * Get a conglomerate descriptor by its UUID String
	 *
	 * @param uuid	The UUID of the conglomerate we're looking for
	 *
	 * @return	The ConglomerateDescriptor if found in this list,
	 *		null if not found.
	 * @exception   StandardException thrown on failure
	 */

	public ConglomerateDescriptor getConglomerateDescriptor(UUID uuid)
						throws StandardException
	{
		ConglomerateDescriptor conglomerateDescriptor;
		ConglomerateDescriptor	returnValue = null;

		int size = size();
		for (int index = 0; index < size; index++)
		{
			conglomerateDescriptor = (ConglomerateDescriptor) get(index);

			if (uuid.equals(conglomerateDescriptor.getUUID()))
			{
				returnValue = conglomerateDescriptor;
				break;
			}
		}

		return returnValue;
	}

    public ConglomerateDescriptor getBaseConglomerateDescriptor() {
        ConglomerateDescriptor conglomerateDescriptor;
        int size = size();
        for (ConglomerateDescriptor conglomerateDescriptor1 : this) {
            conglomerateDescriptor = (ConglomerateDescriptor) conglomerateDescriptor1;
            if (!conglomerateDescriptor.isIndex()) {
                return conglomerateDescriptor;
            }
        }
        throw new RuntimeException("Missing Base Conglomerate");
    }

	/**
	 * Get an array of conglomerate descriptors by a UUID String.  We get
	 * more than one descriptors if duplicate indexes share one conglomerate.
	 *
	 * @param uuid	The UUID of the conglomerate
	 *
	 * @return	Array of ConglomerateDescriptors if found in this list,
	 *		size 0 array if not found.
	 */
	public ConglomerateDescriptor[] getConglomerateDescriptors(UUID uuid)
	{
		ConglomerateDescriptor conglomerateDescriptor;

		int size = size(), j = 0;
		ConglomerateDescriptor[] draft = new ConglomerateDescriptor[size];

		for (int index = 0; index < size; index++)
		{
			conglomerateDescriptor = (ConglomerateDescriptor) get(index);
			if (uuid.equals(conglomerateDescriptor.getUUID()))
				draft[j++] = conglomerateDescriptor;
		}

		if (j == size)
			return draft;
		ConglomerateDescriptor[] returnValue = new ConglomerateDescriptor[j];
        System.arraycopy(draft, 0, returnValue, 0, j);

		return returnValue;
	}

	/**
	 * Remove the specified conglomerate descriptor from the
	 * conglomerate descriptor list.  If the descriptor
	 * is not found, no errors are issued.
	 *
	 * @param tableID table uuid, ignored
	 * @param cgDesc the conglomerate
	 *
	 * @exception   StandardException thrown on failure
	 */
	public void dropConglomerateDescriptor(UUID tableID, ConglomerateDescriptor cgDesc) 
						throws StandardException
	{
		for (Iterator iterator = iterator(); iterator.hasNext(); )
		{
			ConglomerateDescriptor localCgDesc = (ConglomerateDescriptor) iterator.next();
			if (localCgDesc.getConglomerateNumber() == cgDesc.getConglomerateNumber() &&
				localCgDesc.getConglomerateName().equals(cgDesc.getConglomerateName()) &&
				localCgDesc.getSchemaID().equals(cgDesc.getSchemaID()))
			{
				iterator.remove();
				break;
			}
		}
	}

	/**
	 * Remove the specified conglomerate descriptor from the
	 * conglomerate descriptor list.  If the descriptor
	 * is not found, no errors are issued.
	 *
	 * @param conglomerateID table uuid, ignored
	 *
	 * @exception   StandardException thrown on failure
	 */
	public void dropConglomerateDescriptorByUUID(UUID conglomerateID) 
						throws StandardException
	{
		for (Iterator iterator = iterator(); iterator.hasNext(); )
		{
			ConglomerateDescriptor localCgDesc = (ConglomerateDescriptor) iterator.next();
			if ( conglomerateID.equals( localCgDesc.getUUID() ) )
			{
				iterator.remove();
				break;
			}
		}
	}
}
