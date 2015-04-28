package com.splicemachine.test_dao;

import com.google.common.base.Joiner;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.commons.lang.StringUtils.isNotBlank;

public class TriggerBuilder {

    enum Type {
        STATEMENT, ROW
    }

    enum When {
        BEFORE, AFTER
    }

    enum Event {
        INSERT, UPDATE, DELETE
    }

    private Type type;
    private When when;
    private Event event;
    private String triggerName;
    private String tableName;
    private String[] targetColumnNames;
    private String triggerAction;

    public TriggerBuilder reset() {
        type = null;
        when = null;
        event = null;
        tableName = null;
        triggerAction = null;
        triggerName = null;
        targetColumnNames = null;
        return this;
    }

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
        b.append(when == When.BEFORE ? "NO CASCADE BEFORE" : "AFTER");
        b.append(" ");
        b.append(event);
        b.append(" ");
        if (targetColumnNames != null) {
            b.append("OF " + Joiner.on(",").join(targetColumnNames));
            b.append(" ");
        }
        b.append("ON " + tableName);
        b.append(" ");
        b.append(type == Type.STATEMENT ? "FOR EACH STATEMENT" : "FOR EACH ROW");
        b.append(" ");
        b.append(triggerAction);
        return b.toString();
    }

}
