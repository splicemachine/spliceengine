package com.splicemachine.stream.parquet;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

/**
 * Created by jleach on 5/17/15.
 */
public class ParquetExecRowUtils {

    public static MessageType buildSchemaFromExecRowDefinition(ExecRow execRow) throws StandardException {
        /*
        List<Type> types = new ArrayList<Type>(execRow.nColumns());
        for (int i =1 ; i<= execRow.nColumns(); i++) {
            DataValueDescriptor dvd = execRow.getColumn(i);
            if (dvd.getTypeFormatId() == StoredFormatIds.SQL_BOOLEAN_ID)
                types.add(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN,
                        Type.Repetition.OPTIONAL).named(Integer.toString(i)));
            else if (dvd.getTypeFormatId() == StoredFormatIds.SQL_DOUBLE_ID)
                types.add(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE,
                        Type.Repetition.OPTIONAL).named(Integer.toString(i)));
            else if (dvd.getTypeFormatId() == StoredFormatIds.SQL_DECIMAL_ID)
                types.add(Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT,
                        Type.Repetition.OPTIONAL).named(Integer.toString(i)));
            else if (dvd.getTypeFormatId() == StoredFormatIds.SQL_LONGINT_ID)
                types.add(Types.primitive(PrimitiveType.PrimitiveTypeName.INT64,
                        Type.Repetition.OPTIONAL).named(Integer.toString(i)));
            else if (dvd.getTypeFormatId() == StoredFormatIds.SQL_INTEGER_ID)
                types.add(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32,
                        Type.Repetition.OPTIONAL).named(Integer.toString(i)));
            else if (dvd.getTypeFormatId() == StoredFormatIds.SQL_VARCHAR_ID)
                types.add(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY,
                        Type.Repetition.OPTIONAL).as(OriginalType.UTF8).named(Integer.toString(i)));
            else
                types.add(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY,
                        Type.Repetition.OPTIONAL).named(Integer.toString(i)));
        }
        return new MessageType("row",types);
                */
        return null;
    }

    public static void writeRow (ExecRow execRow, RecordConsumer recordConsumer) throws StandardException {
        /*
        recordConsumer.startMessage();
        for (int i =1; i<= execRow.nColumns(); i++) {
            recordConsumer.startField(Integer.toString(i),i-1);
            DataValueDescriptor dvd = execRow.getColumn(i);
            if (dvd.getTypeFormatId() == StoredFormatIds.SQL_BOOLEAN_ID)
                recordConsumer.addBoolean(dvd.getBoolean());
            else if (dvd.getTypeFormatId() == StoredFormatIds.SQL_DOUBLE_ID)
                recordConsumer.addDouble(dvd.getDouble());
            else if (dvd.getTypeFormatId() == StoredFormatIds.SQL_DECIMAL_ID)
                recordConsumer.addFloat(dvd.getFloat());
            else if (dvd.getTypeFormatId() == StoredFormatIds.SQL_LONGINT_ID)
                recordConsumer.addLong(dvd.getLong());
            else if (dvd.getTypeFormatId() == StoredFormatIds.SQL_INTEGER_ID)
                recordConsumer.addInteger(dvd.getInt());
            else if (dvd.getTypeFormatId() == StoredFormatIds.SQL_VARCHAR_ID)
                recordConsumer.addBinary(Binary.fromString(dvd.getString()));
            else {
                recordConsumer.addBinary(Binary.fromByteArray(dvd.getBytes()));
            }
            recordConsumer.endField(Integer.toString(i),i-1);
        }
        recordConsumer.endMessage();
        */

    }

}
