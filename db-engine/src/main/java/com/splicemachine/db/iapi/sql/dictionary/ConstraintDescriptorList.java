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

import com.splicemachine.db.catalog.UUID;

import java.util.ArrayList;

public class ConstraintDescriptorList extends ArrayList<ConstraintDescriptor>
{
    public static final int[] EMPTY = {};

	private boolean scanned;

	/**
	 * Mark whether or not the underlying system table has
	 * been scanned.  (If a table does not have any
	 * constraints then the size of its CDL will always
	 * be 0.  We used these get/set methods to determine
	 * when we need to scan the table.
	 *
	 * @param scanned	Whether or not the underlying system table has been scanned.
	 */
	public void setScanned(boolean scanned)
	{
		this.scanned = scanned;
	}

	/**
	 * Return whether or not the underlying system table has been scanned.
	 *
	 * @return		Where or not the underlying system table has been scanned.
	 */
	public boolean getScanned()
	{
		return scanned;
	}

	/**
	 * Get the ConstraintDescriptor with the matching UUID String for the backing index.
	 *
	 * @param indexUUID		The UUID  for the backing index.
	 *
	 * @return The matching ConstraintDescriptor.
	 */
	public ConstraintDescriptor getConstraintDescriptor(UUID indexUUID)
	{
		ConstraintDescriptor retCD = null;
		int size = size();

		for (int index = 0; index < size; index++)
		{
			ConstraintDescriptor cd = elementAt(index);

			if (! (cd instanceof KeyConstraintDescriptor))
			{
				continue;
			}

			KeyConstraintDescriptor keyCD = (KeyConstraintDescriptor) cd;

			if (keyCD.getIndexId().equals(indexUUID))
			{
				retCD = cd;
				break;
			}
		}
		return retCD;
	}

	/**
	 * Get the ConstraintDescriptor with the matching constraint id.
	 *
	 * @param uuid		The constraint id.
	 *
	 * @return The matching ConstraintDescriptor.
	 */
	public ConstraintDescriptor getConstraintDescriptorById(UUID uuid)
	{
		ConstraintDescriptor returnCD = null;
		int size = size();

		for (int index = 0; index < size; index++)
		{
			ConstraintDescriptor cd = elementAt(index);

			if (cd.getUUID().equals(uuid))
			{
				returnCD = cd;
				break;
			}
		}
		return returnCD;
	}

	/**
	  *	Drop the constraint with the given UUID.
	  *
	  * @param uuid		The constraint id.
	  *
	  * @return The matching ConstraintDescriptor.
	  */
	public ConstraintDescriptor dropConstraintDescriptorById(UUID uuid)
	{
		ConstraintDescriptor cd = null;
		int size = size();

		for (int index = 0; index < size; index++)
		{
			cd = elementAt(index);

			if (cd.getUUID().equals(uuid))
			{
				remove( cd );
				break;
			}
		}

		return cd;
	}



	/**
	 * Get the ConstraintDescriptor with the matching constraint name.
	 *
	 * @param sd		The constraint schema descriptor.
	 * @param name		The constraint name.
	 *
	 * @return The matching ConstraintDescriptor.
	 */
	public ConstraintDescriptor getConstraintDescriptorByName(SchemaDescriptor sd,
																String name)
	{
		ConstraintDescriptor retCD = null;
		int size = size();

		for (int index = 0; index < size; index++)
		{
			ConstraintDescriptor cd = elementAt(index);

			if (cd.getConstraintName().equals(name))
			{
				if ((sd == null) ||
					(sd.equals(cd.getSchemaDescriptor())))
				{
					retCD = cd;
					break;
				}
			}
		}
		return retCD;
	}


	/**
	 * Get the ConstraintDescriptor with the matching constraint name.
	 *
	 * @return The matching ConstraintDescriptor.
	 */
	public ReferencedKeyConstraintDescriptor getPrimaryKey()
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
			ConstraintDescriptor cd = elementAt(index);

			if (cd.getConstraintType() == DataDictionary.PRIMARYKEY_CONSTRAINT)	
			{
				return (ReferencedKeyConstraintDescriptor)cd;
			}
		}
		return (ReferencedKeyConstraintDescriptor)null;
	}

	/**
	 * Return a list of constraints where enabled is
	 * as passed in.
	 *
	 * @param enabled true or false
	 *
	 * @return a constraint descriptor list built from this.  Always
	 * a new list even if all the elements in this were of the correct
	 * type (i.e. not optimized for the case where every element is
	 * desired).
	 */
	public ConstraintDescriptorList getConstraintDescriptorList(boolean enabled)
	{
		ConstraintDescriptorList cdl = new ConstraintDescriptorList();
		int size = size();

		for (int index = 0; index < size; index++)
		{
			ConstraintDescriptor cd = elementAt(index);

			if (cd.isEnabled() == enabled)
			{
				cdl.add(cd);
			}
		}
		return cdl;
	}

	/**
	 * Return the nth (0-based) element in the list.
	 *
	 * @param n	Which element to return.
	 *
	 * @return The nth element in the list.
	 */
	public ConstraintDescriptor elementAt(int n)
	{
		return (ConstraintDescriptor) get(n);
	}

	/**
	 * Return a ConstraintDescriptorList containing the ConstraintDescriptors
	 * of the specified type that are in this list.
	 *
	 * @param type	The constraint type.
	 *
	 * @return A ConstraintDescriptorList containing the ConstraintDescriptors
	 * of the specified type that are in this list.
	 */
	public ConstraintDescriptorList getSubList(int type)
	{
		ConstraintDescriptor cd;
		ConstraintDescriptorList cdl = new ConstraintDescriptorList();
		int size = size();

		for (int index = 0; index < size; index++)
		{
			cd = elementAt(index);

			if (cd.getConstraintType() == type)
			{
				cdl.add(cd);
			}
		}
		return cdl;
	}

    public int[] getBaseColumnOrdering() {
        int[] columnOrdering = EMPTY;
        for (int i = 0; i < size(); i++) {
            ConstraintDescriptor cDescriptor = elementAt(i);
            if (cDescriptor.getConstraintType() == DataDictionary.PRIMARYKEY_CONSTRAINT) {
                int[] referencedColumns = cDescriptor.getReferencedColumns();
                columnOrdering = new int[referencedColumns.length];
                for (int j = 0; j < referencedColumns.length; ++j) {
                    columnOrdering[j] = referencedColumns[j] - 1;
                }
            }
        }
        return columnOrdering;
    }
}
