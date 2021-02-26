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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.compile.StatementNode;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.splicemachine.db.iapi.reference.Property.SPLICE_TIMESTAMP_FORMAT;

public class SPSPropertyManager {
    private static Set<SPSProperty> propertySet = ConcurrentHashMap.newKeySet();
    static {
        propertySet.add(new SPSPropertyTimestampFormat(new BasicUUID("91916f24-fa35-493d-9048-e41ffbe004d7"), SPLICE_TIMESTAMP_FORMAT));
    };

    public static SPSProperty forName(String propertyName) {
        return propertySet.stream().filter(p -> p.name.equals(propertyName)).findAny().orElse(null);
    }

    public static void addDependency(final StatementNode statementNode, CompilerContext cc) throws StandardException {
        for (SPSProperty property : propertySet) {
            property.checkAndAddDependency(statementNode, cc);
        }
    }
}
