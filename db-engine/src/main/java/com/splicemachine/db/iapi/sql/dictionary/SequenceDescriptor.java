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
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.Dependable;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.depend.Dependent;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

/**
 * This class is used by rows in the SYS.SYSSEQUENCES system table.
 * See the header comment of SYSSEQUENCESRowFactory for the
 * contract of that table. In particular, if the CURRENTVALUE column
 * is null, then the sequence has been exhausted and no more values
 * can be generated from it.
 */
public class SequenceDescriptor extends TupleDescriptor
        implements Provider, Dependent, PrivilegedSQLObject
{

    private UUID sequenceUUID;
    private String sequenceName;
    private final SchemaDescriptor schemaDescriptor;
    private UUID schemaId;
    private DataTypeDescriptor dataType;
    private Long currentValue; // could be null
    private long startValue;
    private long minimumValue;
    private long maximumValue;
    private long increment;
    private boolean canCycle;

    /**
     * Constructor
     *
     * @param dataDictionary data dictionary
     * @param sequenceUUID   unique identification in time and space of this sequence
     *                       descriptor
     * @param sequenceName
     */

    public SequenceDescriptor(DataDictionary dataDictionary, SchemaDescriptor sd, UUID sequenceUUID, String sequenceName,
                              DataTypeDescriptor dataType, Long currentValue,
                              long startValue, long minimumValue, long maximumValue, long increment, boolean canCycle) {
        super(dataDictionary);
        if (SanityManager.DEBUG) {
            if (sd.getSchemaName() == null) {
                SanityManager.THROWASSERT("new SequenceDescriptor() schema " +
                        "name is null for Sequence " + sequenceName);
            }
        }
        this.sequenceUUID = sequenceUUID;
        this.schemaDescriptor = sd;
        this.sequenceName = sequenceName;
        this.schemaId = sd.getUUID();
        this.dataType = dataType;
        this.currentValue = currentValue;
        this.startValue = startValue;
        this.minimumValue = minimumValue;
        this.maximumValue = maximumValue;
        this.increment = increment;
        this.canCycle = canCycle;
    }

   /**
	 * @see UniqueTupleDescriptor#getUUID
	 */
	public UUID	getUUID()
	{
		return sequenceUUID;
	}

   /**
	 * @see PrivilegedSQLObject#getObjectTypeName
	 */
	public String getObjectTypeName()
	{
		return PermDescriptor.SEQUENCE_TYPE;
	}

    public String toString() {
        if (SanityManager.DEBUG) {
            return "sequenceUUID: " + sequenceUUID + "\n" +
                    "sequenceName: " + sequenceName + "\n" +
                    "schemaId: " + schemaId + "\n" +
                    "dataType: " + dataType.getTypeName() + "\n" +
                    "currentValue: " + currentValue + "\n" +
                    "startValue: " + startValue + "\n" +
                    "minimumValue: " + minimumValue + "\n" +
                    "maximumValue: " + maximumValue + "\n" +
                    "increment: " + increment + "\n" +
                    "canCycle: " + canCycle + "\n";
        } else {
            return "";
        }
    }

    /**
	 * Check that all of the dependent's dependencies are valid.
	 *
	 * @return true if the dependent is currently valid
	 */
	public synchronized boolean isValid()
	{
		return true;
	}

    /**
	 * Prepare to mark the dependent as invalid (due to at least one of
	 * its dependencies being invalid).
	 *
	 * @param action	The action causing the invalidation
	 * @param p			the provider
	 * @param lcc		the language connection context
	 *
	 * @exception StandardException thrown if unable to make it invalid
	 */
	public void prepareToInvalidate
	(
		Provider 					p,
		int							action,
		LanguageConnectionContext	lcc
	) throws StandardException
	{
		switch (action)
		{   			
			default:
				break;
		}
	}
    /**
	 * Mark the dependent as invalid (due to at least one of
	 * its dependencies being invalid).
	 *
	 * @param 	lcc the language connection context
	 * @param	action	The action causing the invalidation
	 *
	 * @exception StandardException thrown if called in sanity mode
	 */
	public void makeInvalid(int action, LanguageConnectionContext lcc) throws StandardException
	{
		switch (action)
		{
			// invalidate this sequence descriptor
			case DependencyManager.USER_RECOMPILE_REQUEST:
				DependencyManager dm = getDataDictionary().getDependencyManager();
				dm.invalidateFor(this, DependencyManager.PREPARED_STATEMENT_RELEASE, lcc);
				break;

			default:
				break;
		}

	}

    public String getName() {
        return sequenceName;
    }

    public SchemaDescriptor getSchemaDescriptor() throws StandardException {
        return schemaDescriptor;
    }

    /**
     * @see TupleDescriptor#getDescriptorType
     */
    public String getDescriptorType() {
        return "Sequence";
    }

    /**
     * @see TupleDescriptor#getDescriptorName
     */
    public String getDescriptorName() {
        return sequenceName; }

    //////////////////////////////////////////////
    //
    // PROVIDER INTERFACE
    //
    //////////////////////////////////////////////

    /**
     * Get the provider's UUID
     *
     * @return The provider's UUID
     */
    public UUID getObjectID() {
        return sequenceUUID;
    }

    /**
     * Is this provider persistent?  A stored dependency will be required
     * if both the dependent and provider are persistent.
     *
     * @return boolean              Whether or not this provider is persistent.
     */
    public boolean isPersistent() {
        return true;
    }

    /**
     * Return the name of this Provider.  (Useful for errors.)
     *
     * @return String   The name of this provider.
     */
    public String getObjectName() {
        return (sequenceName);
    }

    /**
     * Get the provider's type.
     *
     * @return char         The provider's type.
     */
    public String getClassType() {
        return Dependable.SEQUENCE;
    }

    /**
     * @return the stored form of this provider
     * @see Dependable#getDependableFinder
     */
    public DependableFinder getDependableFinder() {
        return getDependableFinder(
                StoredFormatIds.SEQUENCE_DESCRIPTOR_FINDER_V01_ID);
    }

    /*Accessor methods*/
    public String getSequenceName() {
        return sequenceName;
    }

    public UUID getSchemaId() {
        return schemaId;
    }

    public DataTypeDescriptor getDataType() {
        return dataType;
    }

    public Long getCurrentValue() {
        return currentValue;
    }

    public long getStartValue() {
        return startValue;
    }

    public long getMinimumValue() {
        return minimumValue;
    }

    public long getMaximumValue() {
        return maximumValue;
    }

    public long getIncrement() {
        return increment;
    }

    public boolean canCycle() {
        return canCycle;
    }
}
