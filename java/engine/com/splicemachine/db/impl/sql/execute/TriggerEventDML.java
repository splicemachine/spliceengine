package com.splicemachine.db.impl.sql.execute;

/**
 * Indicates the type of DML statement that causes trigger to fire.
 *
 * @see TriggerEvent
 */
public enum TriggerEventDML {

    UPDATE(1),
    DELETE(2),
    INSERT(4);

    /* The actual ID values are used for serialization and storage in sys.systriggers. Use values that
     * can be stored in and retrieved from integer bitmask. */
    private int id;

    TriggerEventDML(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static TriggerEventDML fromId(int id) {
        for (TriggerEventDML event : TriggerEventDML.values()) {
            if (id == event.getId()) {
                return event;
            }
        }
        throw new IllegalArgumentException("id=" + id);
    }
}
