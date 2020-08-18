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

package com.splicemachine.ck.decoder;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.catalog.SYSSCHEMASRowFactory;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.SerializerMap;
import com.splicemachine.derby.utils.marshall.dvd.V1SerializerMap;
import com.splicemachine.utils.Pair;

public class SysSchemasDataDecoder extends UserDataDecoder {
    @Override
    public Pair<ExecRow, DescriptorSerializer[]> getExecRowAndDescriptors() {
        ExecRow er = new ValueRow(SYSSCHEMASRowFactory.SYSSCHEMAS_COLUMN_COUNT);
        SYSSCHEMASRowFactory.setRowColumns(er, null, null, null);
        SerializerMap serializerMap = new V1SerializerMap(false);
        return new Pair<>(er, serializerMap.getSerializers(er));
    }
}
