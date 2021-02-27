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

import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.Dependent;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;

import java.sql.Timestamp;

public class StatementCacheValue implements Dependent {
    private final Timestamp timestamp;
    private final GenericStorablePreparedStatement statement;

    public StatementCacheValue(GenericStorablePreparedStatement statement, Timestamp timestamp) {
        this.statement = statement;
        this.timestamp = timestamp;
    }

    public StatementCacheValue(GenericStorablePreparedStatement statement) {
        this(statement, new Timestamp(System.currentTimeMillis()));
    }

    public GenericStorablePreparedStatement getStatement() {
        return statement;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean isValid() {
        return statement.isValid(); // delegation
    }

    @Override
    public void prepareToInvalidate(Provider p, int action, LanguageConnectionContext lcc) throws StandardException {
        statement.prepareToInvalidate(p, action, lcc); // delegation
    }

    @Override
    public void makeInvalid(int action, LanguageConnectionContext lcc) throws StandardException {
        statement.makeInvalid(action, lcc); // delegation
    }

    @Override
    public DependableFinder getDependableFinder() {
        return statement.getDependableFinder(); // delegation
    }

    @Override
    public String getObjectName() {
        return statement.getObjectName(); // delegation
    }

    @Override
    public UUID getObjectID() {
        return statement.getObjectID(); // delegation
    }

    @Override
    public boolean isPersistent() {
        return statement.isPersistent(); // delegation
    }

    @Override
    public String getClassType() {
        return statement.getClassType(); // delegation
    }
}
