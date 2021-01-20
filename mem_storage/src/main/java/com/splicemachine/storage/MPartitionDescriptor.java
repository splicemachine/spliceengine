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

package com.splicemachine.storage;

import com.splicemachine.si.constants.SIConstants;

import java.util.HashMap;
import java.util.Map;

public class MPartitionDescriptor implements PartitionDescriptor {

    private Map<String, String> values;

    public MPartitionDescriptor(String schemaDisplayName,
                                String tableDisplayName,
                                String indexDisplayName) {
        values = new HashMap<>(3);
        values.put(SIConstants.SCHEMA_DISPLAY_NAME_ATTR, schemaDisplayName);
        values.put(SIConstants.TABLE_DISPLAY_NAME_ATTR, tableDisplayName);
        values.put(SIConstants.INDEX_DISPLAY_NAME_ATTR, indexDisplayName);
    }

    @Override
    public String getValue(String key) {
        if(key == null) {
            return null;
        }
        return values.get(key); // null if not found.
    }
}
