package com.splicemachine.test_dao;

import com.google.common.base.Joiner;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.commons.lang.StringUtils.isNotBlank;

/**
 * Example create trigger SQL:
 * <p/>
 * <pre>
 *  name:                 CREATE TRIGGER trig1
 *  when:                 AFTER
 *  event:                UPDATE
 *  target columns:       OF a,b,c
 *  target table:         ON t1
 *  referencing clause:   REFERENCING OLD AS OLD_ROW
 *  type:                 FOR EACH ROW
 *  action:               INSERT INTO trigger_count VALUES('afterUpdate_' || OLD_ROW.b );
 * </pre>
 */
public class TriggerBuilder {

    enum When {
        BEFORE, AFTER
    }

    enum Event {
        INSERT, UPDATE, DELETE
    }

    enum Type {
        STATEMENT, ROW
    }

    private String triggerName;
    private When when;
    private Event event;
    /* Name of the table upon which the trigger is defined */
    private String tableName;
    /* Name of the table columns upon which the trigger is defined (optional) */
    private String[] targetColumnNames;
    /* For example: 'REFERENCING NEW AS NEW_ROW' */
    private String referencingClause;
    private Type type;
    private String triggerAction;

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // when
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public TriggerBuilder before() {
        this.when = When.BEFORE;
        return this;
    }

    public TriggerBuilder after() {
        this.when = When.AFTER;
        return this;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // trigger type
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public TriggerBuilder row() {
        this.type = Type.ROW;
        return this;
    }

    public TriggerBuilder statement() {
        this.type = Type.STATEMENT;
        return this;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // events
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public TriggerBuilder delete() {
        this.event = Event.DELETE;
        return this;
    }

    public TriggerBuilder update() {
        this.event = Event.UPDATE;
        return this;
    }

    public TriggerBuilder insert() {
        this.event = Event.INSERT;
        return this;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public TriggerBuilder on(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public TriggerBuilder then(String triggerAction) {
        this.triggerAction = triggerAction;
        return this;
    }

    public TriggerBuilder named(String triggerName) {
        this.triggerName = triggerName;
        return this;
    }

    public TriggerBuilder of(String... targetColumnNames) {
        this.targetColumnNames = targetColumnNames;
        return this;
    }

    public TriggerBuilder referencing(String referencingClause) {
        this.referencingClause = referencingClause;
        return this;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public String build() {
        checkState(isNotBlank(triggerName), "trigger name is required");
        checkState(isNotBlank(tableName), "table name is required");

        StringBuilder b = new StringBuilder();
        b.append("CREATE TRIGGER");
        b.append(" ");
        b.append(triggerName);
        b.append(" ");
        b.append(when);
        b.append(" ");
        b.append(event);
        b.append(" ");
        if (targetColumnNames != null) {
            b.append("OF " + Joiner.on(",").join(targetColumnNames));
            b.append(" ");
        }
        b.append("ON " + tableName);
        b.append(" ");
        if (referencingClause != null) {
            b.append("REFERENCING " + referencingClause);
            b.append(" ");
        }
        b.append(type == Type.STATEMENT ? "FOR EACH STATEMENT" : "FOR EACH ROW");
        b.append(" ");
        b.append(triggerAction);
        return b.toString();
    }

}
