package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.execute.ScanQualifier;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.impl.sql.execute.GenericScanQualifier;
import java.math.BigDecimal;
import java.util.GregorianCalendar;

/**
 * @author Scott Fines
 *         Created on: 10/1/13
 */
public class QualifierUtils {

    private QualifierUtils(){}

    /**
     * Adjusts a qualifier to make sure that it's assumed column type matches the column
     * format specified by the table (e.g. if the table stores floats, then make sure that
     * the qualifier is also a float).
     *
     * This prevents issues with incorrect scans due to serialization issues.
     *
     * @param qualifier the qualifier to adjust
     * @param columnFormat the typeFormatId of the actual stored column
     * @return an adjusted scan qualifier with the proper column type
     * @throws StandardException if something goes wrong.
     */
    public static Qualifier adjustQualifier(Qualifier qualifier, int columnFormat,DataValueFactory dataValueFactory) throws StandardException {
        if(isFloatType(columnFormat)){
            return convertFloatingPoint(qualifier,columnFormat,dataValueFactory);
        }else if(isScalarType(columnFormat)){
            return convertScalar(qualifier,columnFormat,dataValueFactory);
        }else if(isTimestamp(columnFormat)){
            return convertTimestamp(qualifier,columnFormat,dataValueFactory);
        }else if(isDate(columnFormat)){
            return convertDate(qualifier,columnFormat,dataValueFactory);
        }else if(isTime(columnFormat)){
            return convertTime(qualifier,columnFormat,dataValueFactory);            
        }else return qualifier; //nothing to do
    }
    
    public static DataValueDescriptor adjustDataValueDescriptor(DataValueDescriptor dvd, int columnFormat,DataValueFactory dataValueFactory) throws StandardException {
        if(isFloatType(columnFormat)){
            return convertFloatingPoint(dvd,columnFormat,dataValueFactory);
        }else if(isScalarType(columnFormat)){
            return convertScalar(dvd,columnFormat,dataValueFactory);
        }else if(isTimestamp(columnFormat)){
            return convertTimestamp(dvd,columnFormat,dataValueFactory);
        }else if(isDate(columnFormat)){
            return convertDate(dvd,columnFormat,dataValueFactory);
        }else if(isTime(columnFormat)){
            return convertTime(dvd,columnFormat,dataValueFactory);            
        }else return dvd; //nothing to do
    }
    
    private static Qualifier reTypeQualifier(Qualifier qualifier,DataValueDescriptor correctType) {
    	if(qualifier instanceof ScanQualifier){
            ((ScanQualifier)qualifier).setQualifier(qualifier.getColumnId(),
                    correctType,
                    qualifier.getOperator(),
                    qualifier.negateCompareResult(),
                    qualifier.getOrderedNulls(),
                    qualifier.getUnknownRV());
        }else{
            //make it an instanceof ScanQualifier
            ScanQualifier qual = new GenericScanQualifier();
            qual.setQualifier(qualifier.getColumnId(),
                    correctType,
                    qualifier.getOperator(),
                    qualifier.negateCompareResult(),
                    qualifier.getOrderedNulls(),
                    qualifier.getUnknownRV());
            qualifier = qual;
        }
        return qualifier;
    }
    
    private static Qualifier convertTime(Qualifier qualifier, int columnFormat,DataValueFactory dataValueFactory) throws StandardException {
        return reTypeQualifier(qualifier,convertTime(qualifier.getOrderable(),columnFormat,dataValueFactory));
    }

    private static DataValueDescriptor convertTime(DataValueDescriptor dvd, int correctColumnFormat, DataValueFactory dataValueFactory) throws StandardException {
        DataValueDescriptor correctType = dataValueFactory.getNull(correctColumnFormat, -1);
        correctType.setValue(dvd.getTime(new GregorianCalendar()));
        return correctType;
    }
   
    private static Qualifier convertDate(Qualifier qualifier, int columnFormat,DataValueFactory dataValueFactory) throws StandardException {
        return reTypeQualifier(qualifier,convertDate(qualifier.getOrderable(),columnFormat,dataValueFactory));
    }

    private static DataValueDescriptor convertDate(DataValueDescriptor dvd, int correctColumnFormat, DataValueFactory dataValueFactory) throws StandardException {
        DataValueDescriptor correctType = dataValueFactory.getNull(correctColumnFormat, -1);
        correctType.setValue(dvd.getDate(new GregorianCalendar()));
        return correctType;
    }

    
    private static Qualifier convertTimestamp(Qualifier qualifier, int columnFormat,DataValueFactory dataValueFactory) throws StandardException {
        return reTypeQualifier(qualifier,convertTimestamp(qualifier.getOrderable(),columnFormat,dataValueFactory));
    }
    
    private static DataValueDescriptor convertTimestamp(DataValueDescriptor dvd, int correctColumnFormat, DataValueFactory dataValueFactory) throws StandardException {
        DataValueDescriptor correctType = dataValueFactory.getNull(correctColumnFormat, -1);
        correctType.setValue(dvd.getTimestamp(new GregorianCalendar()));
        return correctType;
    }


    private static DataValueDescriptor convertScalar(DataValueDescriptor dvd, int columnFormat, DataValueFactory dataValueFactory) throws StandardException {
    	 /*
         * Technically, all Scalar types encode the same way. However, that's an implementation
         * detail which may change in the future (particularly with regards to small data types,
         * like TINYINT which can serialize more compactly while retaining order characteristics).
         * Thus, this method does two things:
         *
         * 1. Convert decimal types into the correct Scalar type (truncation)
         * 2. Convert into correct Scalar types to adjust for potential overflow issues (ints bigger
         * than Short.MAX_VALUE, etc).
         */
        DataValueDescriptor correctType = dataValueFactory.getNull(columnFormat, -1);
        double value;
        int currentTypeFormatId = dvd.getTypeFormatId();
        switch(currentTypeFormatId){
            case StoredFormatIds.SQL_DOUBLE_ID: //return new SQLDouble();
                value = dvd.getDouble();
                break;
            case StoredFormatIds.SQL_TINYINT_ID: //return new SQLTinyint();
                value = dvd.getByte();
                break;
            case StoredFormatIds.SQL_SMALLINT_ID: //return new SQLSmallint();
                value = dvd.getShort();
                break;
            case StoredFormatIds.SQL_INTEGER_ID: //return new SQLInteger();
                value = dvd.getInt();
                break;
            case StoredFormatIds.SQL_REAL_ID: //return new SQLReal();
                value = dvd.getFloat();
                break;
            case StoredFormatIds.SQL_DECIMAL_ID:
                BigDecimal val = (BigDecimal)dvd.getObject();
                value = val.doubleValue();
                break;
            default:
                value = dvd.getLong();
        }
        double maxValue = Long.MAX_VALUE;
        double minValue = Long.MIN_VALUE;
        if(columnFormat==StoredFormatIds.SQL_INTEGER_ID){
            maxValue = Integer.MAX_VALUE;
            minValue = Integer.MIN_VALUE;
        }else if(columnFormat==StoredFormatIds.SQL_SMALLINT_ID){
            maxValue = Short.MAX_VALUE;
            minValue = Short.MIN_VALUE;
        }else if(columnFormat==StoredFormatIds.SQL_TINYINT_ID){
            maxValue = Byte.MAX_VALUE;
            minValue = Byte.MIN_VALUE;
        }

        if(value > maxValue)
            value = maxValue;
        else if(value < minValue)
            value = minValue;
        correctType.setValue(value);
        return correctType;
    }
    
    private static Qualifier convertScalar(Qualifier qualifier, int columnFormat,DataValueFactory dataValueFactory) throws StandardException {
        DataValueDescriptor correctType = convertScalar(qualifier.getOrderable(), columnFormat,dataValueFactory);
        if(qualifier instanceof ScanQualifier){
            ((ScanQualifier)qualifier).setQualifier(qualifier.getColumnId(),
                    correctType,
                    qualifier.getOperator(),
                    qualifier.negateCompareResult(),
                    qualifier.getOrderedNulls(),
                    qualifier.getUnknownRV());
        }else{
            //make it an instanceof ScanQualifier
            ScanQualifier qual = new GenericScanQualifier();
            qual.setQualifier(qualifier.getColumnId(),
                    correctType,
                    qualifier.getOperator(),
                    qualifier.negateCompareResult(),
                    qualifier.getOrderedNulls(),
                    qualifier.getUnknownRV());
            qualifier = qual;
        }
        return qualifier;
    }


    private static boolean isFloatType(int columnFormat){
        return (columnFormat==StoredFormatIds.SQL_REAL_ID
                ||columnFormat==StoredFormatIds.SQL_DECIMAL_ID
                || columnFormat==StoredFormatIds.SQL_DOUBLE_ID);
    }

    private static boolean isTimestamp(int columnFormat){
        return columnFormat==StoredFormatIds.SQL_TIMESTAMP_ID;
    }

    private static boolean isTime(int columnFormat){
        return columnFormat==StoredFormatIds.SQL_TIME_ID;
    }

    private static boolean isDate(int columnFormat){
        return columnFormat==StoredFormatIds.SQL_DATE_ID;
    }
    
    private static boolean isScalarType(int columnFormat){
        return (columnFormat==StoredFormatIds.SQL_TINYINT_ID
                || columnFormat==StoredFormatIds.SQL_SMALLINT_ID
                || columnFormat==StoredFormatIds.SQL_INTEGER_ID
                || columnFormat==StoredFormatIds.SQL_LONGINT_ID);
    }

    private static DataValueDescriptor convertFloatingPoint(DataValueDescriptor dvd, int columnFormat,DataValueFactory dataValueFactory) throws StandardException {
        DataValueDescriptor correctType = dataValueFactory.getNull(columnFormat,-1);
        int currentTypeFormatId= dvd.getTypeFormatId();
        if(isScalarType(currentTypeFormatId)){
            //getLong() runs no risk of overflow from a scalar type, so we can set it and be done
            correctType.setValue(dvd.getLong());
        }else{
            /*
             * Since floats, doubles, and BigDecimals all serialize differently, we have to be
             * concerned about upcasting as well as overflow. That is, if we are scanning a double
             * column, but we have a float scan qualifier, we have to upcast that float into a double.
             */
            if(currentTypeFormatId==StoredFormatIds.SQL_REAL_ID){
                correctType.setValue(dvd.getFloat());
            }else if(currentTypeFormatId==StoredFormatIds.SQL_DOUBLE_ID){
                if(columnFormat==StoredFormatIds.SQL_REAL_ID){
                    //check for float overflow
                    double value = dvd.getDouble();
                    if(value > Limits.DB2_LARGEST_REAL){
                        value = Float.MAX_VALUE;
                    }else if(value < Limits.DB2_SMALLEST_POSITIVE_REAL){
                        value = 0f;
                    }
                    correctType.setValue(value);
                }else if(columnFormat==StoredFormatIds.SQL_DECIMAL_ID){
                    correctType.setValue(dvd.getDouble());
                }
            }else if(currentTypeFormatId==StoredFormatIds.SQL_DECIMAL_ID){
                BigDecimal val = (BigDecimal)dvd.getObject();
                double value = 0;
                if(columnFormat==StoredFormatIds.SQL_REAL_ID){
                    if(val.compareTo(BigDecimal.valueOf(Float.MAX_VALUE))>0)
                        value = Float.MAX_VALUE;
                    else if(val.signum()>0 &&val.compareTo(BigDecimal.valueOf(Limits.DB2_SMALLEST_POSITIVE_REAL))<0)
                        value = 0f;
                    else if(val.signum()<0 &&val.compareTo(BigDecimal.valueOf(Limits.DB2_LARGEST_NEGATIVE_REAL))>0)
                        value = 0f;
                    else
                        value = val.floatValue();
                }else if(columnFormat==StoredFormatIds.SQL_DOUBLE_ID){
                    if(val.compareTo(BigDecimal.valueOf(Limits.DB2_LARGEST_DOUBLE))>0)
                        value = Double.MAX_VALUE;
                    else if(val.signum()>0 && val.compareTo(BigDecimal.valueOf(Limits.DB2_SMALLEST_POSITIVE_DOUBLE))<0)
                        value = 0d;
                    else if(val.signum()<0 && val.compareTo(BigDecimal.valueOf(Limits.DB2_LARGEST_NEGATIVE_DOUBLE))>0)
                        value = 0d;
                    else
                        value = val.doubleValue();
                }else{
                	value = val.doubleValue(); // hmm this could be wrong
                }
                correctType.setValue(value);
            }
        }
        return correctType;
    }

    
    private static Qualifier convertFloatingPoint(Qualifier qualifier, int columnFormat,DataValueFactory dataValueFactory) throws StandardException {
        DataValueDescriptor correctType = convertFloatingPoint(qualifier.getOrderable(), columnFormat, dataValueFactory);
        /*
         * We must check for overflow amongst decimal types, but we don't need to worry about overflowing
         * from scalar types.
         */
        if(qualifier instanceof ScanQualifier){
            ((ScanQualifier)qualifier).setQualifier(qualifier.getColumnId(),
                    correctType,
                    qualifier.getOperator(),
                    qualifier.negateCompareResult(),
                    qualifier.getOrderedNulls(),
                    qualifier.getUnknownRV());
        }else{
            //make it an instanceof ScanQualifier
            ScanQualifier qual = new GenericScanQualifier();
            qual.setQualifier(qualifier.getColumnId(),
                    correctType,
                    qualifier.getOperator(),
                    qualifier.negateCompareResult(),
                    qualifier.getOrderedNulls(),
                    qualifier.getUnknownRV());
            qualifier = qual;
        }
        return qualifier;
    }
}
