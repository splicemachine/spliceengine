package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.dictionary.GenericDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TriggerDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TriggerDescriptorV4;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

public final class TriggerInfoV2 extends TriggerInfo {

    /**
     * Default constructor for Formattable
     */
    public TriggerInfoV2() {
    }

    public TriggerInfoV2(TriggerInfo v1)
    {
        triggerDescriptors = v1.triggerDescriptors;
        columnNames = v1.columnNames;
        columnIds = v1.columnIds;
        hasSpecialFromTableTrigger = false;
    }

    /**
     * Constructor for TriggerInfo
     *
     * @param td          the table upon which the trigger is declared
     * @param changedCols the columns that are changed in the dml that is causing the trigger to fire
     * @param triggers    the list of trigger descriptors
     */
    public TriggerInfoV2(TableDescriptor td, int[] changedCols, GenericDescriptorList<TriggerDescriptor> triggers) {

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
            SanityManager.ASSERT(triggers != null && !triggers.isEmpty(), "trigger descriptor list has no elements");
        }

        /* Copy the trigger descriptors into an array of the right type */
        Iterator<TriggerDescriptor> descIter = triggers.iterator();
        int size = triggers.size();
        triggerDescriptors = new TriggerDescriptor[size];
        for (int i = 0; i < size; i++) {
            triggerDescriptors[i] = descIter.next();
            if (triggerDescriptors[i] instanceof TriggerDescriptorV4) {
                TriggerDescriptorV4 triggerDesc = (TriggerDescriptorV4) triggerDescriptors[i];
                if (triggerDesc.isSpecialFromTableTrigger())
                    hasSpecialFromTableTrigger = true;
            }
        }
    }

    //////////////////////////////////////////////
    //
    // FORMATABLE v2
    //
    //////////////////////////////////////////////

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(hasSpecialFromTableTrigger);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        hasSpecialFromTableTrigger = in.readBoolean();
    }

    @Override
    public int getTypeFormatId() {
        return StoredFormatIds.TRIGGER_INFO_V02_ID;
    }

}
