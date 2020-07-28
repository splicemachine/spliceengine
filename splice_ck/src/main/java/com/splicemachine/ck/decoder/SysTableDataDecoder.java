package com.splicemachine.ck.decoder;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.catalog.SYSTABLESRowFactory;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.SerializerMap;
import com.splicemachine.derby.utils.marshall.dvd.V1SerializerMap;
import com.splicemachine.utils.Pair;

public class SysTableDataDecoder extends UserDataDecoder {
    @Override
    protected Pair<ExecRow, DescriptorSerializer[]> getExecRowAndDescriptors() {
        ExecRow er = new ValueRow(SYSTABLESRowFactory.SYSTABLES_COLUMN_COUNT);
        SYSTABLESRowFactory.setRowColumns(er, null, null, null, null,
                null, null, -1, null, null, null,
                null, null, null, false, false);
        SerializerMap serializerMap = new V1SerializerMap(false);
        return new Pair<>(er, serializerMap.getSerializers(er));
    }
}
