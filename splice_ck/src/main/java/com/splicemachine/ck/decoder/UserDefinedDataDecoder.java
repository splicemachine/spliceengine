package com.splicemachine.ck.decoder;

import com.splicemachine.ck.Utils;
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
    protected Pair<ExecRow, DescriptorSerializer[]> getExecRowAndDescriptors() {
        // map strings to format ids.
        int[] storedFormatIds = new int[schema.length];
        DataValueDescriptor[] dataValueDescriptors = new DataValueDescriptor[schema.length];
        for(int i=0; i < storedFormatIds.length; ++i) {
            switch (schema[i]) {
                case INT:
                    storedFormatIds[i] = StoredFormatIds.SQL_INTEGER_ID;
                    dataValueDescriptors[i] = new SQLInteger();
                    break;
                case BIGINT:
                    storedFormatIds[i] = StoredFormatIds.SQL_LONGINT_ID;
                    dataValueDescriptors[i] = new SQLLongint();
                    break;
                case DOUBLE:
                    storedFormatIds[i] = StoredFormatIds.SQL_DOUBLE_ID;
                    dataValueDescriptors[i] = new SQLDouble();
                    break;
                case CHAR:
                    storedFormatIds[i] = StoredFormatIds.SQL_CHAR_ID;
                    dataValueDescriptors[i] = new SQLChar();
                    break;
                case VARCHAR:
                    storedFormatIds[i] = StoredFormatIds.SQL_VARCHAR_ID;
                    dataValueDescriptors[i] = new SQLVarchar();
                    break;
                case TIME:
                    storedFormatIds[i] = StoredFormatIds.SQL_TIME_ID;
                    dataValueDescriptors[i] = new SQLTime();
                    break;
                case TIMESTAMP:
                    storedFormatIds[i] = StoredFormatIds.SQL_TIMESTAMP_ID;
                    dataValueDescriptors[i] = new SQLTimestamp();
                    break;
                case DATE:
                    storedFormatIds[i] = StoredFormatIds.SQL_DATE_ID;
                    dataValueDescriptors[i] = new SQLDate();
                    break;
                case DECIMAL:
                    storedFormatIds[i] = StoredFormatIds.SQL_DECIMAL_ID;
                    dataValueDescriptors[i] = new SQLDecimal();
                    break;
                default:
                    throw new RuntimeException("type not supported");
            }
        }

        SerializerMap serializerMap = null;
        if(version == 1) {
            serializerMap = new V1SerializerMap(false);
        } else if (version == 2) {
            serializerMap = new V2SerializerMap(false);
        } else if (version == 3) {
            serializerMap = new V3SerializerMap(false);
        } else {
            serializerMap = new V4SerializerMap(false);
        }
        return new Pair<>(new ValueRow(dataValueDescriptors), serializerMap.getSerializers(storedFormatIds));
    }
}
