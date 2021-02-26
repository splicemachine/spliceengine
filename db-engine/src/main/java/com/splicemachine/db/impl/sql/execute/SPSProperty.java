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

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.impl.sql.compile.StatementNode;

import java.util.Objects;

/**
 * This class represents a system property that is used internally in an SPS (Stored Procedure Statement), it is used
 * to model an SPS dependency on a property such that we can invalidate it when the property is changed by the user.
 * For example, if the user defines a timestamp format property and we have an SPS the contains a timestamp column we
 * create an SPSProperty for the timestamp property and add the SPS as Dependent on it, later on, when the user modifies
 * the timestamp format, we use the DependencyManager to invalidate all SPS dependents on that property.
 */
public abstract class SPSProperty implements Provider {

    final UUID uuid;
    final String name;

    protected SPSProperty(UUID uuid, String name) {
        assert name != null;
        assert uuid != null;

        this.uuid = uuid;
        this.name = name;
    }

    @Override
    public DependableFinder getDependableFinder() {
        return null;
    }

    @Override
    public String getObjectName() {
        return name;
    }

    @Override
    public UUID getObjectID() {
        return uuid;
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public String getClassType() {
        return SPS_PROPERTY;
    }

    protected abstract void checkAndAddDependency(final StatementNode statementNode, CompilerContext cc) throws StandardException;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SPSProperty property = (SPSProperty) o;
        return name.equals(property.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
