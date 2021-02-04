/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.impl.sql.GenericStatement;

import java.sql.Timestamp;
import java.util.Objects;

public class StatementKey {
    private final Timestamp timestamp;
    private final GenericStatement statement;

    public StatementKey(GenericStatement statement, Timestamp timestamp) {
        this.statement = statement;
        this.timestamp = timestamp;
    }

    public StatementKey(GenericStatement statement) {
        this(statement, new Timestamp(System.currentTimeMillis()));
    }

    @Override
    public String toString() {
        return statement.getStatementText();
    }

    public GenericStatement getStatement() {
        return statement;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatementKey that = (StatementKey) o;
        return Objects.equals(statement, that.statement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statement);
    }
}
