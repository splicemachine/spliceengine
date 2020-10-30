package com.splicemachine.db.iapi.types;

import com.google.protobuf.ExtensionRegistry;
import com.splicemachine.db.catalog.types.CatalogMessage;
import com.splicemachine.utils.ByteSlice;

public class ProtoBufUtils {

    public static ExtensionRegistry createDVDExtensionRegistry() {
        ExtensionRegistry extensionRegistry = ExtensionRegistry.newInstance();
        extensionRegistry.add(CatalogMessage.SQLTinyint.sqlTinyint);
        extensionRegistry.add(CatalogMessage.SQLSmallint.sqlSmallint);
        extensionRegistry.add(CatalogMessage.SQLInteger.sqlInteger);
        extensionRegistry.add(CatalogMessage.SQLLongint.sqlLongint);
        extensionRegistry.add(CatalogMessage.SQLReal.sqlReal);
        extensionRegistry.add(CatalogMessage.SQLDouble.sqlDouble);
        extensionRegistry.add(CatalogMessage.SQLDecimal.sqlDecimal);
        extensionRegistry.add(CatalogMessage.SQLDecfloat.sqlDecfloat);
        extensionRegistry.add(CatalogMessage.SQLDate.sqlDate);
        extensionRegistry.add(CatalogMessage.SQLTime.sqlTime);
        extensionRegistry.add(CatalogMessage.SQLTimestamp.sqlTimestamp);
        extensionRegistry.add(CatalogMessage.SQLBoolean.sqlBoolean);
        extensionRegistry.add(CatalogMessage.SQLChar.sqlChar);
        extensionRegistry.add(CatalogMessage.SQLBinary.sqlBinary);
        extensionRegistry.add(CatalogMessage.ListDataType.listDataType);
        extensionRegistry.add(CatalogMessage.SQLArray.sqlArray);
        extensionRegistry.add(CatalogMessage.XML.xml);
        extensionRegistry.add(CatalogMessage.UserType.userType);
        extensionRegistry.add(CatalogMessage.SQLRef.sqlRef);
        extensionRegistry.add(CatalogMessage.SQLRowId.sqlRowId);
        extensionRegistry.add(CatalogMessage.HBaseRowLocation.hbaseRowLocation);
        extensionRegistry.add(CatalogMessage.HBaseConglomerate.hbaseConglomerate);
        extensionRegistry.add(CatalogMessage.IndexConglomerate.indexConglomerate);
        return extensionRegistry;
    }
    public static DataValueDescriptor fromProtobuf(CatalogMessage.DataValueDescriptor d) {

        DataValueDescriptor dvd = null;
        CatalogMessage.DataValueDescriptor.Type type = d.getType();
        switch (type) {
            case SQLTinyint:
                CatalogMessage.SQLTinyint sqlTinyint = d.getExtension(CatalogMessage.SQLTinyint.sqlTinyint);
                dvd = new SQLTinyint(sqlTinyint);
                break;
            case SQLSmallint:
                CatalogMessage.SQLSmallint smallint = d.getExtension(CatalogMessage.SQLSmallint.sqlSmallint);
                dvd = new SQLSmallint(smallint);
                break;
            case SQLInteger:
                CatalogMessage.SQLInteger sqlInteger = d.getExtension(CatalogMessage.SQLInteger.sqlInteger);
                dvd = new SQLInteger(sqlInteger);
                break;
            case SQLLongint:
                CatalogMessage.SQLLongint longint = d.getExtension(CatalogMessage.SQLLongint.sqlLongint);
                dvd = new SQLLongint(longint);
                break;
            case SQLReal:
                CatalogMessage.SQLReal sqlReal = d.getExtension(CatalogMessage.SQLReal.sqlReal);
                dvd = new SQLReal(sqlReal);
                break;
            case SQLDouble:
                CatalogMessage.SQLDouble sqlDouble = d.getExtension(CatalogMessage.SQLDouble.sqlDouble);
                dvd = new SQLDouble(sqlDouble);
                break;
            case SQLDecimal:
                CatalogMessage.SQLDecimal sqlDecimal = d.getExtension(CatalogMessage.SQLDecimal.sqlDecimal);
                dvd = new SQLDecimal(sqlDecimal);
                break;
            case SQLDecfloat:
                CatalogMessage.SQLDecfloat sqlDecfloat = d.getExtension(CatalogMessage.SQLDecfloat.sqlDecfloat);
                dvd = new SQLDecfloat(sqlDecfloat);
                break;
            case SQLDate:
                CatalogMessage.SQLDate sqlDate = d.getExtension(CatalogMessage.SQLDate.sqlDate);
                dvd = new SQLDate(sqlDate);
                break;
            case SQLTime:
                CatalogMessage.SQLTime sqlTime = d.getExtension(CatalogMessage.SQLTime.sqlTime);
                dvd = new SQLTime(sqlTime);
                break;
            case SQLTimestamp:
                CatalogMessage.SQLTimestamp sqlTimestamp = d.getExtension(CatalogMessage.SQLTimestamp.sqlTimestamp);
                dvd = new SQLTimestamp(sqlTimestamp);
                break;
            case SQLBoolean:
                CatalogMessage.SQLBoolean sqlBoolean = d.getExtension(CatalogMessage.SQLBoolean.sqlBoolean);
                dvd = new SQLBoolean(sqlBoolean);
                break;
            case SQLChar:
                CatalogMessage.SQLChar sqlChar = d.getExtension(CatalogMessage.SQLChar.sqlChar);
                dvd = fromProtobuf(sqlChar);
                break;
            case SQLBinary:
                CatalogMessage.SQLBinary sqlBinary = d.getExtension(CatalogMessage.SQLBinary.sqlBinary);
                dvd = fromProtobuf(sqlBinary);
                break;
            case SQLArray:
                CatalogMessage.SQLArray sqlArray = d.getExtension(CatalogMessage.SQLArray.sqlArray);
                dvd = new SQLArray(sqlArray);
                break;
            case ListDataType:
                CatalogMessage.ListDataType listDataType = d.getExtension(CatalogMessage.ListDataType.listDataType);
                dvd = new ListDataType(listDataType);
                break;
            case XML:
                CatalogMessage.XML xml = d.getExtension(CatalogMessage.XML.xml);
                dvd = new XML(xml);
                break;
            case UserType:
                CatalogMessage.UserType userType = d.getExtension(CatalogMessage.UserType.userType);
                dvd = new UserType(userType);
                break;
            case SQLRef:
                CatalogMessage.SQLRef sqlRef = d.getExtension(CatalogMessage.SQLRef.sqlRef);
                dvd = new SQLRef(sqlRef);
                break;
            case SQLRowId:
                CatalogMessage.SQLRowId sqlRowId = d.getExtension(CatalogMessage.SQLRowId.sqlRowId);
                dvd = new SQLRowId(sqlRowId);
                break;
            case HBaseRowLocation:
                CatalogMessage.HBaseRowLocation rowLocation =
                        d.getExtension(CatalogMessage.HBaseRowLocation.hbaseRowLocation);
                dvd = new HBaseRowLocation(rowLocation);
                break;
            case HBaseConglomerate:
            case IndexConglomerate:
            default:
                throw new RuntimeException("Unexpected type " + type);
        }
        return dvd;
    }

    public static DataValueDescriptor fromProtobuf(CatalogMessage.SQLBinary sqlBinary) {
        CatalogMessage.SQLBinary.Type type = sqlBinary.getType();
        DataValueDescriptor dvd = null;
        switch (type) {
            case SQLBit:
                dvd = new SQLBit(sqlBinary);
                break;
            case SQLBlob:
                dvd = new SQLBlob(sqlBinary);
                break;
            case SQLLongVarbit:
                dvd = new SQLLongVarbit(sqlBinary);
                break;
            case SQLVarbit:
                dvd = new SQLVarbit(sqlBinary);
                break;
            default:
                throw new RuntimeException("Unexpected type " + type);
        }
        return dvd;
    }

    public static DataValueDescriptor fromProtobuf(CatalogMessage.SQLChar sqlChar) {
        DataValueDescriptor dvd = null;
        if (!sqlChar.hasType()) {
            dvd = new SQLChar(sqlChar);
            return dvd;
        }
        CatalogMessage.SQLChar.Type type = sqlChar.getType();
        switch (type) {
            case SQLClob:
                dvd = new SQLClob(sqlChar);
                break;
            case SQLLongVarchar:
                dvd = new SQLLongvarchar(sqlChar);
                break;
            case SQLVarchar:
                dvd = new SQLVarchar(sqlChar);
                break;
            case SQLVarcharDB2Compatible:
                dvd = new SQLVarcharDB2Compatible(sqlChar);
                break;
            case CollatorSQLChar:
                dvd = new CollatorSQLChar(sqlChar);
                break;
            case CollatorSQLVarchar:
                dvd = new CollatorSQLVarchar(sqlChar);
                break;
            case CollatorSQLLongVarchar:
                dvd = new CollatorSQLLongvarchar(sqlChar);
                break;
            case CollatorSQLClob:
                dvd = new CollatorSQLClob(sqlChar);
                break;
            case CollatorSQLVarcharDB2Compatible:
                dvd = new CollatorSQLVarcharDB2Compatible(sqlChar);
                break;
            default:
                throw new RuntimeException("Unexpected type " + type);
        }

        return dvd;
    }
    public static ByteSlice fromProtobuf(CatalogMessage.ByteSlice bs) {
        return new ByteSlice(bs);
    }
}
