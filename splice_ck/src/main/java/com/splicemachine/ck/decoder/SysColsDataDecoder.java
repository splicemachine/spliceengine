package com.splicemachine.ck.decoder;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.catalog.SYSCOLUMNSRowFactory;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.SerializerMap;
import com.splicemachine.derby.utils.marshall.dvd.V1SerializerMap;
import com.splicemachine.utils.Pair;

public class SysColsDataDecoder extends UserDataDecoder {
    @Override
    public Pair<ExecRow, DescriptorSerializer[]> getExecRowAndDescriptors() {
        ExecRow er = new ValueRow(SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMN_COUNT);
        SYSCOLUMNSRowFactory.setRowColumns(er, null, null, null, null, -1,
                null, null, -1, -1, -1, -1,
                -1, false, (byte) 0b0);
        SerializerMap serializerMap = new V1SerializerMap(false);
        return new Pair<>(er, serializerMap.getSerializers(er));
    }
}
