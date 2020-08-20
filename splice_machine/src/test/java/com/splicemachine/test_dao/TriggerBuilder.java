/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.test_dao;

import splice.com.google.common.base.Joiner;

import static splice.com.google.common.base.Preconditions.checkArgument;
import static splice.com.google.common.base.Preconditions.checkNotNull;
import static splice.com.google.common.base.Preconditions.checkState;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

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

    private static final String DEFAULT_TRIGGER_NAME = "triggerBuilderDefaultTriggerName";

    enum When {
        BEFORE, AFTER
    }

    enum Event {
        INSERT, UPDATE, DELETE
    }

    enum Type {
        STATEMENT, ROW
    }

    private String triggerName = DEFAULT_TRIGGER_NAME;
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
        checkState(isNotBlank(tableName), "trigger table name is required");
        checkState(isNotBlank(triggerAction), "trigger action is required");
        checkNotNull(when, "trigger (BEFORE/AFTER) required");
        checkNotNull(event, "trigger event (INSERT/UPDATE/DELETE) required");
        checkNotNull(type, "trigger type (ROW/STATEMENT) required");

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
