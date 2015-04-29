/*

   Derby - Class org.apache.derby.impl.sql.execute.TriggerInfo

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.sql.dictionary.GenericDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TriggerDescriptor;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.Formatable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.util.Iterator;

/**
 * This is a simple class used to store the run time information
 * about a foreign key.  Used by DML to figure out what to
 * check.
 */
public final class TriggerInfo implements Formatable {

    private TriggerDescriptor[] triggerDescriptors;
    private String[] columnNames;
    private int[] columnIds;

    /**
     * Default constructor for Formattable
     */
    public TriggerInfo() {
    }

    /**
     * Constructor for TriggerInfo
     *
     * @param td          the table upon which the trigger is declared
     * @param changedCols the columns that are changed in the dml that is causing the trigger to fire
     * @param triggers    the list of trigger descriptors
     */
    public TriggerInfo(TableDescriptor td, int[] changedCols, GenericDescriptorList<TriggerDescriptor> triggers) {

        this.columnIds = changedCols;
        if (columnIds != null) {
            /* Find the names of all the columns that are being changed. */
            columnNames = new String[columnIds.length];
            for (int i = 0; i < columnIds.length; i++) {
                columnNames[i] = td.getColumnDescriptor(columnIds[i]).getColumnName();
            }
        }

        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(triggers != null, "null trigger descriptor list");
            SanityManager.ASSERT(triggers != null && triggers.size() > 0, "trigger descriptor list has no elements");
        }

        /* Copy the trigger descriptors into an array of the right type */
        Iterator<TriggerDescriptor> descIter = triggers.iterator();
        int size = triggers.size();
        triggerDescriptors = new TriggerDescriptor[size];
        for (int i = 0; i < size; i++) {
            triggerDescriptors[i] = descIter.next();
        }
    }

    /*
     * private constructor for TriggerInfo
     */
    private TriggerInfo(TriggerDescriptor[] triggers, int[] changedColsIds, String[] changedColsNames) {
        this.columnIds = changedColsIds;
        this.columnNames = changedColsNames;
        this.triggerDescriptors = triggers;
    }

    public boolean hasBeforeStatementTrigger() {
        return hasTrigger(true, false);
    }

    public boolean hasBeforeRowTrigger() {
        return hasTrigger(true, true);
    }

    public boolean hasAfterStatementTrigger() {
        return hasTrigger(false, false);
    }

    public boolean hasAfterRowTrigger() {
        return hasTrigger(false, true);
    }

    /**
     * Do we have a trigger or triggers that meet the criteria
     *
     * @param isBefore true for a before trigger, false for after trigger, null for either
     * @param isRow    true for a row trigger, false for statement trigger, null for either
     * @return true if we have a trigger that meets the criteria
     */
    boolean hasTrigger(boolean isBefore, boolean isRow) {
        if (triggerDescriptors != null) {
            for (TriggerDescriptor aTriggerArray : triggerDescriptors) {
                if ((aTriggerArray.isBeforeTrigger() == isBefore) && (aTriggerArray.isRowTrigger() == isRow)) {
                    return true;
                }
            }
        }
        return false;
    }

    TriggerDescriptor[] getTriggerDescriptors() {
        return triggerDescriptors;
    }

    public int[] getColumnIds() {
        return columnIds;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    //////////////////////////////////////////////
    //
    // java.lang.Object
    //
    //////////////////////////////////////////////


    @Override
    public String toString() {
        if (SanityManager.DEBUG) {
            StringBuilder str = new StringBuilder();
            str.append("\nColumn names modified:\t\t(");
            for (int i = 0; i < columnNames.length; i++) {
                if (i > 0)
                    str.append(",");

                str.append(columnNames[i]);
            }
            str.append(")");

            str.append("\nColumn ids modified:\t\t(");
            for (int i = 0; i < columnIds.length; i++) {
                if (i > 0)
                    str.append(",");

                str.append(columnIds[i]);
            }
            str.append(")");

            str.append("\nTriggers:");
            for (TriggerDescriptor aTriggerArray : triggerDescriptors) {
                str.append("\n" + aTriggerArray);
            }
            return str.toString();
        } else {
            return "";
        }
    }


    //////////////////////////////////////////////
    //
    // FORMATABLE
    //
    //////////////////////////////////////////////

    /**
     * Write this object out
     *
     * @param out write bytes here
     * @throws IOException thrown on error
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        ArrayUtil.writeArray(out, triggerDescriptors);
        ArrayUtil.writeIntArray(out, columnIds);
        ArrayUtil.writeArray(out, columnNames);
    }

    /**
     * Read this object from a stream of stored objects.
     *
     * @param in read this.
     * @throws IOException            thrown on error
     * @throws ClassNotFoundException thrown on error
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        triggerDescriptors = new TriggerDescriptor[ArrayUtil.readArrayLength(in)];
        ArrayUtil.readArrayItems(in, triggerDescriptors);

        columnIds = ArrayUtil.readIntArray(in);

        int len = ArrayUtil.readArrayLength(in);
        if (len > 0) {
            columnNames = new String[len];
            ArrayUtil.readArrayItems(in, columnNames);
        }
    }

    /**
     * Get the formatID which corresponds to this class.
     *
     * @return the formatID of this class
     */
    @Override
    public int getTypeFormatId() {
        return StoredFormatIds.TRIGGER_INFO_V01_ID;
    }
}
