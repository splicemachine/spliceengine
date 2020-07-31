package com.splicemachine.ck.decoder;

import com.splicemachine.ck.Utils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.marshall.dvd.*;
import com.splicemachine.utils.Pair;

public class UserDefinedDataDecoder extends UserDataDecoder {

    private final Utils.SQLType[] schema;
    private final int version;

    public UserDefinedDataDecoder(Utils.SQLType[] schema, int version) {
        this.schema = schema;
        this.version = version;
    }

    @Override
    public Pair<ExecRow, DescriptorSerializer[]> getExecRowAndDescriptors() throws StandardException {
        return Utils.constructExecRowDescriptorSerializer(schema, version, null);
    }
}
