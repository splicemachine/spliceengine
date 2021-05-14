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

package com.splicemachine.derby.procedures;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.impl.sql.catalog.Procedure;
import com.splicemachine.primitives.Bytes;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 7/31/14
 */
public class GenericSpliceFunctions {
    public static List<Procedure> getProcedures() throws StandardException {
        return Arrays.asList(
                //
                // General functions
                //
                Procedure.newBuilder().name("LONG_UUID")
                        .numOutputParams(1)
                        .numResultSets(0)
                        .sqlControl(RoutineAliasInfo.NO_SQL)
                        .returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                        .isDeterministic(false).ownerClass(GenericSpliceFunctions.class.getCanonicalName())
                        .build()
        );
    }

    private GenericSpliceFunctions(){} //don't instantiate utility class

    public static long LONG_UUID(){
        return Bytes.toLong(EngineDriver.driver().newUUIDGenerator(1).nextBytes());
    }
}
