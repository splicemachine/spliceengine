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
import com.splicemachine.db.impl.sql.execute.TriggerEventDML;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.List;

/**
 * Version 2 of the Trigger Descriptor class.
 * Added because SerDe doesn't have a version number stored in it,
 * to know how many fields to deserialize, and this could be stored
 * on disk in sys.sysstatements.
 * Instead, we store the version number in the class name, and
 * add the new fields to serialize in this class.
 */
public class TriggerDescriptorV3 extends TriggerDescriptor {

    /**
     * Default constructor for formatable
     */
    public TriggerDescriptorV3() {
    }

    /**
     * Constructor.  Used when creating a trigger from SYS.SYSTRIGGERS
     *
     * @param dataDictionary                the data dictionary
     * @param sd                            the schema descriptor for this trigger
     * @param id                            the trigger id
     * @param name                          the trigger name
     * @param eventMask                     TriggerDescriptor.TRIGGER_EVENT_XXXX
     * @param isBefore                      is this a before (as opposed to after) trigger
     * @param isRow                         is this a row trigger or statement trigger
     * @param isEnabled                     is this trigger enabled or disabled
     * @param td                            the table upon which this trigger is defined
     * @param whenSPSId                     the sps id for the when clause (may be null)
     * @param actionSPSIdList               the spsid for the trigger action (may be null)
     * @param creationTimestamp             when was this trigger created?
     * @param referencedCols                what columns does this trigger reference (may be null)
     * @param referencedColsInTriggerAction what columns does the trigger
     *                                      action reference through old/new transition variables (may be null)
     * @param triggerDefinitionList         The original user text of the trigger action
     * @param referencingOld                whether or not OLD appears in REFERENCING clause
     * @param referencingNew                whether or not NEW appears in REFERENCING clause
     * @param oldReferencingName            old referencing table name, if any, that appears in REFERCING clause
     * @param newReferencingName            new referencing table name, if any, that appears in REFERCING clause
     * @param whenClauseText                the SQL text of the WHEN clause, or {@code null}
     *                                      if there is no WHEN clause
     */
    public TriggerDescriptorV3(
            DataDictionary dataDictionary,
            SchemaDescriptor sd,
            UUID id,
            String name,
            TriggerEventDML eventMask,
            boolean isBefore,
            boolean isRow,
            boolean isEnabled,
            TableDescriptor td,
            UUID whenSPSId,
            List<UUID> actionSPSIdList,
            Timestamp creationTimestamp,
            int[] referencedCols,
            int[] referencedColsInTriggerAction,
            List<String> triggerDefinitionList,
            boolean referencingOld,
            boolean referencingNew,
            String oldReferencingName,
            String newReferencingName,
            String whenClauseText) {
        super(
            dataDictionary,
            sd,
            id,
            name,
            eventMask,
            isBefore,
            isRow,
            isEnabled,
            td,
            whenSPSId,
            actionSPSIdList,
            creationTimestamp,
            referencedCols,
            referencedColsInTriggerAction,
            triggerDefinitionList,
            referencingOld,
            referencingNew,
            oldReferencingName,
            newReferencingName,
            whenClauseText);
        this.version = 3;
    }

    //////////////////////////////////////////////////////////////
    //
    // FORMATABLE
    //
    //////////////////////////////////////////////////////////////
    /**
     * WARNING - If adding new fields to readExternal/writeExternal
     * you must create a new subclass, and add the fields there.
     * Otherwise old SPSDescriptors stored in sys.sysstatements may
     * fail to deserialize properly.
     *
     * @param in read this.
     * @throws IOException            thrown on error
     * @throws ClassNotFoundException thrown on error
     */
    /**
     * Read this object from a stream of stored objects.
     *
     * @param in read this.
     * @throws IOException            thrown on error
     * @throws ClassNotFoundException thrown on error
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        version = in.readInt();

        Object obj = in.readObject();
        if (obj instanceof List) {
            assert triggerDefinitionList == null : "triggerDefinition and triggerDefinitionList cannot both be defined";
            triggerDefinitionList = (List<String>) obj;
        }

        obj = in.readObject();
        if (obj instanceof List) {
            assert actionSPSIdList == null : "actionSPSIdList and actionSPSId cannot both be defined";
            actionSPSIdList= (List<UUID>) obj;
        }

    }

    /**
     * Write this object to a stream of stored objects.
     *
     * @param out write bytes here.
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(version);
        out.writeObject(triggerDefinitionList.size() > 1 ? triggerDefinitionList : null);
        out.writeObject(actionSPSIdList.size() > 1 ? actionSPSIdList : null);
    }
}

