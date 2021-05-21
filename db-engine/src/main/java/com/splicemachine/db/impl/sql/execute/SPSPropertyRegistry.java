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
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Node;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.compile.StatementNode;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A static registry that keeps track of all SPS properties and checks whether a dependency should be created
 * from a statement plan to a specific SPS property.
 *
 * If you add a new SPS property, make sure to register it here so it is checked when analyzing a statement plan.
 */
public class SPSPropertyRegistry {
    private static Set<SPSProperty> propertySet = ConcurrentHashMap.newKeySet();
    static {
        propertySet.add(new SPSPropertyTimestampFormat(new BasicUUID("91916f24-fa35-493d-9048-e41ffbe004d7"), Property.SPLICE_TIMESTAMP_FORMAT));
        propertySet.add(new SPSPropertyCurrentTimestampFormat(new BasicUUID("b2decb1d-acc2-4402-9278-d4aab1fc6431"), Property.SPLICE_CURRENT_TIMESTAMP_PRECISION));
        propertySet.add(new SPSPropertyFloatingpointNotation(new BasicUUID("e8027088-78d8-41aa-be8e-ede5c646ae0a"), Property.FLOATING_POINT_NOTATION));
        propertySet.add(new SPSPropertySecondFunction(new BasicUUID("01fc4415-bc53-4ac1-b829-93e0e61b0c5f"), Property.SPLICE_SECOND_FUNCTION_COMPATIBILITY_MODE));
        propertySet.add(new SPSPropertyCountReturnType(new BasicUUID("20e3d160-b514-4e82-b8a0-3a7888c253c5"), Property.COUNT_RETURN_TYPE));
    };

    public static SPSProperty forName(String propertyName) {
        return propertySet.stream().filter(p -> p.name.equals(propertyName)).findAny().orElse(null);
    }

    /**
     * Adds a dependency if necessary between a statement node and an SPS property using the compiler's context
     * dependency manager.
     */
    public static void checkAndAddDependency(final Node node, CompilerContext cc) throws StandardException {
        assert node != null;
        assert cc != null;

        for (SPSProperty property : propertySet) {
            property.checkAndAddDependency(node, cc);
        }
    }
}
