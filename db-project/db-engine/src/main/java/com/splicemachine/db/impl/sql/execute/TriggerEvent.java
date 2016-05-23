package com.splicemachine.db.impl.sql.execute;

import static com.splicemachine.db.impl.sql.execute.TriggerEventDML.*;
import static com.splicemachine.db.impl.sql.execute.TriggerEventWhen.AFTER;
import static com.splicemachine.db.impl.sql.execute.TriggerEventWhen.BEFORE;

/**
 * This is a simple class that we use to track trigger events.  This is not expected to be used directly, instead there
 * is a static TriggerEvent in TriggerEvents for each event found in this file.
 */
public enum TriggerEvent {

    BEFORE_INSERT(BEFORE, INSERT),
    BEFORE_UPDATE(BEFORE, UPDATE),
    BEFORE_DELETE(BEFORE, DELETE),

    AFTER_INSERT(AFTER, INSERT),
    AFTER_UPDATE(AFTER, UPDATE),
    AFTER_DELETE(AFTER, DELETE);

    private TriggerEventWhen when;
    private TriggerEventDML dml;

    /**
     * Create a trigger event of the given type
     */
    TriggerEvent(TriggerEventWhen when, TriggerEventDML dml) {
        this.when = when;
        this.dml = dml;
    }

    /**
     * Get the type number of this trigger
     */
    public String getName() {
        return when + " " + dml;
    }

    /**
     * Is this a before trigger
     */
    public boolean isBefore() {
        return when == BEFORE;
    }

    /**
     * Is this an after trigger
     */
    public boolean isAfter() {
        return when == AFTER;
    }

    /**
     * Was it an update event that caused this trigger?
     * @return true if was an update.
     */
    public boolean isUpdate() {
        return dml == TriggerEventDML.UPDATE;
    }

    public TriggerEventDML getDml() {
        return dml;
    }
}
