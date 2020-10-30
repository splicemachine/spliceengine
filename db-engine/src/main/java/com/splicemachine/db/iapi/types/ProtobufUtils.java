package com.splicemachine.db.iapi.types;

import com.google.protobuf.ExtensionRegistry;
import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.*;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.util.ByteArray;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.*;
import com.splicemachine.db.impl.sql.execute.FKInfo;
import com.splicemachine.db.impl.sql.execute.TriggerInfo;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;

public class ProtobufUtils {

    public static ExtensionRegistry createAggregatorExtensionRegistry() {
        ExtensionRegistry extensionRegistry = createDVDExtensionRegistry();
        extensionRegistry.add(CatalogMessage.CountAggregator.countAggregator);
        extensionRegistry.add(CatalogMessage.AvgAggregator.avgAggregator);
        extensionRegistry.add(CatalogMessage.DecimalBufferedSumAggregator.decimalBufferedSumAggregator);
        extensionRegistry.add(CatalogMessage.DoubleBufferedSumAggregator.doubleBufferedSumAggregator);
        extensionRegistry.add(CatalogMessage.LongBufferedSumAggregator.longBufferedSumAggregator);
        extensionRegistry.add(CatalogMessage.MaxMinAggregator.maxMinAggregator);
        extensionRegistry.add(CatalogMessage.StringAggregator.stringAggregator);
        extensionRegistry.add(CatalogMessage.SumAggregator.sumAggregator);

        return extensionRegistry;
    }

    public static ExtensionRegistry createDVDExtensionRegistry() {
        ExtensionRegistry extensionRegistry = ExtensionRegistry.newInstance();
        extensionRegistry.add(TypeMessage.SQLTinyint.sqlTinyint);
        extensionRegistry.add(TypeMessage.SQLSmallint.sqlSmallint);
        extensionRegistry.add(TypeMessage.SQLInteger.sqlInteger);
        extensionRegistry.add(TypeMessage.SQLLongint.sqlLongint);
        extensionRegistry.add(TypeMessage.SQLReal.sqlReal);
        extensionRegistry.add(TypeMessage.SQLDouble.sqlDouble);
        extensionRegistry.add(TypeMessage.SQLDecimal.sqlDecimal);
        extensionRegistry.add(TypeMessage.SQLDecfloat.sqlDecfloat);
        extensionRegistry.add(TypeMessage.SQLDate.sqlDate);
        extensionRegistry.add(TypeMessage.SQLTime.sqlTime);
        extensionRegistry.add(TypeMessage.SQLTimestamp.sqlTimestamp);
        extensionRegistry.add(TypeMessage.SQLBoolean.sqlBoolean);
        extensionRegistry.add(TypeMessage.SQLChar.sqlChar);
        extensionRegistry.add(TypeMessage.SQLBinary.sqlBinary);
        extensionRegistry.add(TypeMessage.ListDataType.listDataType);
        extensionRegistry.add(TypeMessage.SQLArray.sqlArray);
        extensionRegistry.add(TypeMessage.XML.xml);
        extensionRegistry.add(TypeMessage.UserType.userType);
        extensionRegistry.add(TypeMessage.SQLRef.sqlRef);
        extensionRegistry.add(TypeMessage.SQLRowId.sqlRowId);
        extensionRegistry.add(TypeMessage.HBaseRowLocation.hbaseRowLocation);
        extensionRegistry.add(TypeMessage.SpliceConglomerate.spliceConglomerate);
        extensionRegistry.add(TypeMessage.HBaseConglomerate.hbaseConglomerate);
        extensionRegistry.add(TypeMessage.IndexConglomerate.indexConglomerate);
        return extensionRegistry;
    }
    public static DataValueDescriptor fromProtobuf(TypeMessage.DataValueDescriptor d) {

        DataValueDescriptor dvd = null;
        TypeMessage.DataValueDescriptor.Type type = d.getType();
        switch (type) {
            case SQLTinyint:
                TypeMessage.SQLTinyint sqlTinyint = d.getExtension(TypeMessage.SQLTinyint.sqlTinyint);
                dvd = new SQLTinyint(sqlTinyint);
                break;
            case SQLSmallint:
                TypeMessage.SQLSmallint smallint = d.getExtension(TypeMessage.SQLSmallint.sqlSmallint);
                dvd = new SQLSmallint(smallint);
                break;
            case SQLInteger:
                TypeMessage.SQLInteger sqlInteger = d.getExtension(TypeMessage.SQLInteger.sqlInteger);
                dvd = new SQLInteger(sqlInteger);
                break;
            case SQLLongint:
                TypeMessage.SQLLongint longint = d.getExtension(TypeMessage.SQLLongint.sqlLongint);
                dvd = new SQLLongint(longint);
                break;
            case SQLReal:
                TypeMessage.SQLReal sqlReal = d.getExtension(TypeMessage.SQLReal.sqlReal);
                dvd = new SQLReal(sqlReal);
                break;
            case SQLDouble:
                TypeMessage.SQLDouble sqlDouble = d.getExtension(TypeMessage.SQLDouble.sqlDouble);
                dvd = new SQLDouble(sqlDouble);
                break;
            case SQLDecimal:
                TypeMessage.SQLDecimal sqlDecimal = d.getExtension(TypeMessage.SQLDecimal.sqlDecimal);
                dvd = new SQLDecimal(sqlDecimal);
                break;
            case SQLDecfloat:
                TypeMessage.SQLDecfloat sqlDecfloat = d.getExtension(TypeMessage.SQLDecfloat.sqlDecfloat);
                dvd = new SQLDecfloat(sqlDecfloat);
                break;
            case SQLDate:
                TypeMessage.SQLDate sqlDate = d.getExtension(TypeMessage.SQLDate.sqlDate);
                dvd = new SQLDate(sqlDate);
                break;
            case SQLTime:
                TypeMessage.SQLTime sqlTime = d.getExtension(TypeMessage.SQLTime.sqlTime);
                dvd = new SQLTime(sqlTime);
                break;
            case SQLTimestamp:
                TypeMessage.SQLTimestamp sqlTimestamp = d.getExtension(TypeMessage.SQLTimestamp.sqlTimestamp);
                dvd = new SQLTimestamp(sqlTimestamp);
                break;
            case SQLBoolean:
                TypeMessage.SQLBoolean sqlBoolean = d.getExtension(TypeMessage.SQLBoolean.sqlBoolean);
                dvd = new SQLBoolean(sqlBoolean);
                break;
            case SQLChar:
                TypeMessage.SQLChar sqlChar = d.getExtension(TypeMessage.SQLChar.sqlChar);
                dvd = fromProtobuf(sqlChar);
                break;
            case SQLBinary:
                TypeMessage.SQLBinary sqlBinary = d.getExtension(TypeMessage.SQLBinary.sqlBinary);
                dvd = fromProtobuf(sqlBinary);
                break;
            case SQLArray:
                TypeMessage.SQLArray sqlArray = d.getExtension(TypeMessage.SQLArray.sqlArray);
                dvd = new SQLArray(sqlArray);
                break;
            case ListDataType:
                TypeMessage.ListDataType listDataType = d.getExtension(TypeMessage.ListDataType.listDataType);
                dvd = new ListDataType(listDataType);
                break;
            case XML:
                TypeMessage.XML xml = d.getExtension(TypeMessage.XML.xml);
                dvd = new XML(xml);
                break;
            case UserType:
                TypeMessage.UserType userType = d.getExtension(TypeMessage.UserType.userType);
                dvd = new UserType(userType);
                break;
            case SQLRef:
                TypeMessage.SQLRef sqlRef = d.getExtension(TypeMessage.SQLRef.sqlRef);
                dvd = new SQLRef(sqlRef);
                break;
            case SQLRowId:
                TypeMessage.SQLRowId sqlRowId = d.getExtension(TypeMessage.SQLRowId.sqlRowId);
                dvd = new SQLRowId(sqlRowId);
                break;
            case HBaseRowLocation:
                TypeMessage.HBaseRowLocation rowLocation =
                        d.getExtension(TypeMessage.HBaseRowLocation.hbaseRowLocation);
                dvd = new HBaseRowLocation(rowLocation);
                break;
            case SpliceConglomerate:
            default:
                throw new RuntimeException("Unexpected type " + type);
        }
        return dvd;
    }

    public static DataValueDescriptor fromProtobuf(TypeMessage.SQLBinary sqlBinary) {
        TypeMessage.SQLBinary.Type type = sqlBinary.getType();
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

    public static DataValueDescriptor fromProtobuf(TypeMessage.SQLChar sqlChar) {
        DataValueDescriptor dvd = null;
        if (!sqlChar.hasType()) {
            dvd = new SQLChar(sqlChar);
            return dvd;
        }
        TypeMessage.SQLChar.Type type = sqlChar.getType();
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
    public static ByteSlice fromProtobuf(TypeMessage.ByteSlice bs) {
        return new ByteSlice(bs);
    }

    public static IndexDescriptor fromProtobuf(CatalogMessage.IndexDescriptorImpl id) throws IOException {
        return new IndexDescriptorImpl(id);
    }

    public static UUID fromProtobuf(com.splicemachine.db.impl.sql.CatalogMessage.UUID uuid) {
        return new BasicUUID(uuid);
    }

    public static TriggerDescriptor fromProtobuf(CatalogMessage.TriggerDescriptor triggerDescriptor) {
        if (!triggerDescriptor.hasType()) {
            return new TriggerDescriptor(triggerDescriptor);
        }
        else {
            CatalogMessage.TriggerDescriptor.Type type = triggerDescriptor.getType();
            switch(type) {
                case TriggerDescriptorV2:
                    return new TriggerDescriptorV2(triggerDescriptor);
                case TriggerDescriptorV3:
                    return new TriggerDescriptorV3(triggerDescriptor);
                case TriggerDescriptorV4:
                    return new TriggerDescriptorV4(triggerDescriptor);
                default:
                    throw new RuntimeException("Unexpected type " + type.name());
            }
        }
    }

    public static GenericColumnDescriptor fromProtobuf(
            CatalogMessage.ResultColumnDescriptor resultColumnDescriptor) throws IOException{
        return new GenericColumnDescriptor(resultColumnDescriptor);
    }

    public static IndexRowGenerator fromProtobuf(CatalogMessage.IndexRowGenerator indexRowGenerator) throws IOException{
        return new IndexRowGenerator(indexRowGenerator);
    }

    public static FKInfo fromProtobuf(CatalogMessage.FKInfo fkInfo) throws IOException {
        return new FKInfo(fkInfo);
    }

    public static TriggerInfo fromProtobuf(CatalogMessage.TriggerInfo triggerInfo) {
        return new TriggerInfo(triggerInfo);
    }

    public static FormatableBitSet fromProtobuf(CatalogMessage.FormatableBitSet bitSet) {
        return new FormatableBitSet(bitSet);
    }

    public static RowLocation fromProtobuf(TypeMessage.HBaseRowLocation rowLocation) {
        return new HBaseRowLocation(rowLocation);
    }

    public static ResultDescription fromProtobuf(CatalogMessage.ResultDescription resultDescription) throws IOException{
        return new GenericResultDescription(resultDescription);
    }

    public static CursorInfo fromProtobuf(CatalogMessage.CursorInfo cursorInfo) throws IOException {
        return new CursorInfo(cursorInfo);
    }

    public static ByteArray fromProtobuf(CatalogMessage.ByteArray byteArray) {
        return new ByteArray(byteArray);
    }

    public static CursorTableReference fromProtobuf(CatalogMessage.CursorTableReference cursorTableReference) {
        return new CursorTableReference(cursorTableReference);
    }

    public static DataTypeDescriptor fromProtobuf(CatalogMessage.DataTypeDescriptor dataTypeDescriptor) throws IOException {
       return new DataTypeDescriptor(dataTypeDescriptor);
    }

    public static TypeDescriptorImpl fromProtobuf(CatalogMessage.TypeDescriptorImpl typeDescriptor) {
        return new TypeDescriptorImpl(typeDescriptor);
    }
    public static BaseTypeIdImpl fromProtobuf(CatalogMessage.BaseTypeIdImpl baseTypeId) {
        CatalogMessage.BaseTypeIdImpl.Type t = baseTypeId.getType();
        switch (t) {
            case UserDefinedTypeIdImpl:
                return new UserDefinedTypeIdImpl(baseTypeId);
            case RowMultiSetImpl:
                return new RowMultiSetImpl(baseTypeId);
            case DecimalTypeIdImpl:
                return new DecimalTypeIdImpl(baseTypeId);
            case BaseTypeIdImpl:
            default:
                return new BaseTypeIdImpl(baseTypeId);
        }
    }
}
