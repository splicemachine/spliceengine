/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.types;

import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.types.BaseTypeIdImpl;
import com.splicemachine.db.catalog.types.RowMultiSetImpl;
import com.splicemachine.db.catalog.types.TypeDescriptorImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.loader.ClassInspector;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import org.apache.spark.sql.types.StructField;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Types;
import java.text.RuleBasedCollator;

import static com.splicemachine.db.iapi.types.TypeId.CHAR_ID;

/**
 * DataTypeDescriptor describes a runtime SQL type.
 * It consists of a catalog type (TypeDescriptor)
 * and runtime attributes. The list of runtime
 * attributes is:
 * <UL>
 * <LI> Collation Derivation
 * </UL>
 * <p/>
 * A DataTypeDescriptor is immutable.
 */
public class DataTypeDescriptor implements Formatable{
    /********************************************************
     **
     **    This class implements Formatable. That means that it
     **    can write itself to and from a formatted stream. If
     **    you add more fields to this class, make sure that you
     **    also write/read them with the writeExternal()/readExternal()
     **    methods.
     **
     **    If, inbetween releases, you add more fields to this class,
     **    then you should bump the version number emitted by the getTypeFormatId()
     **    method.
     **
     ********************************************************/

    /**
     * Runtime INTEGER type that is nullable.
     */
    public static final DataTypeDescriptor INTEGER=new DataTypeDescriptor(TypeId.INTEGER_ID,true);

    /**
     * Runtime INTEGER type that is not nullable.
     */
    public static final DataTypeDescriptor INTEGER_NOT_NULL=INTEGER.getNullabilityType(false);

    /**
     * Runtime SMALLINT type that is nullable.
     */
    public static final DataTypeDescriptor SMALLINT=new DataTypeDescriptor(TypeId.SMALLINT_ID,true);

    /**
     * Runtime INTEGER type that is not nullable.
     */
    public static final DataTypeDescriptor SMALLINT_NOT_NULL=SMALLINT.getNullabilityType(false);

    public static final DataTypeDescriptor DOUBLE=new DataTypeDescriptor(TypeId.DOUBLE_ID,true);

    public static final DataTypeDescriptor LONGINT=new DataTypeDescriptor(TypeId.BIGINT_ID,true);

    public static final DataTypeDescriptor LONGINT_NOT_NULL=LONGINT.getNullabilityType(false);

    /*
    ** Static creators
    */

    /**
     * Get a descriptor that corresponds to a nullable builtin JDBC type.
     * If a variable length type then the size information will be set
     * to the maximum possible.
     * <p/>
     * Collation type will be UCS_BASIC and derivation IMPLICIT.
     * <p/>
     * For well known types code may also use the pre-defined
     * runtime types that are fields of this class, such as INTEGER.
     *
     * @param jdbcType The int type of the JDBC type for which to get
     *                 a corresponding SQL DataTypeDescriptor
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     * A null return value means there is no corresponding SQL type
     */
    public static DataTypeDescriptor getBuiltInDataTypeDescriptor ( int jdbcType ){
        return DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdbcType,true);
    }

    /**
     * Get a descriptor that corresponds to a nullable builtin variable
     * length JDBC type.
     * <p/>
     * Collation type will be UCS_BASIC and derivation IMPLICIT.
     *
     * @param jdbcType The int type of the JDBC type for which to get
     *                 a corresponding SQL DataTypeDescriptor
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     * A null return value means there is no corresponding SQL type
     */
    public static DataTypeDescriptor getBuiltInDataTypeDescriptor ( int jdbcType, int length ){
        return DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdbcType,true,length);
    }

    /**
     * Return a runtime type for a catalog type.
     */
    public static DataTypeDescriptor getType(TypeDescriptor catalogType){
        TypeDescriptorImpl typeDescriptor=(TypeDescriptorImpl)catalogType;

        TypeId typeId=TypeId.getTypeId(catalogType);

        DataTypeDescriptor dtd=
                new DataTypeDescriptor(typeDescriptor,typeId);

        // By definition, any catalog type (column in a table,
        // procedure etc.) is derivation implicit.
        dtd.collationDerivation=StringDataValue.COLLATION_DERIVATION_IMPLICIT;

        return dtd;
    }

    /**
     * Return a nullable catalog type for a JDBC builtin type and length.
     */
    public static TypeDescriptor getCatalogType(int jdbcType,int length){
        return getBuiltInDataTypeDescriptor(jdbcType,length).getCatalogType();
    }

    /**
     * Return a nullable catalog type for a fixed length JDBC builtin type.
     */
    public static TypeDescriptor getCatalogType(int jdbcType){
        return getBuiltInDataTypeDescriptor(jdbcType).getCatalogType();
    }

    /**
     * Get a catlog type identical to the passed in type exception
     * that the collationType is set to the passed in value.
     *
     * @param catalogType   Type to be based upon.
     * @param collationType Collation type of returned type.
     * @return catalogType if it already has the correct collation,
     * otherwise a new TypeDescriptor with the correct collation.
     */
    public static TypeDescriptor getCatalogType(TypeDescriptor catalogType, int collationType){
        if(catalogType.isRowMultiSet())
            return getRowMultiSetCollation(catalogType,collationType);

        if(catalogType.getCollationType()==collationType)
            return catalogType;

        // Create through a runtime type, derivation will be thrown away.
        return getType(catalogType).getCollatedType(collationType,
                StringDataValue.COLLATION_DERIVATION_IMPLICIT).getCatalogType();
    }

    /**
     * Get a descriptor that corresponds to a builtin JDBC type.
     * <p/>
     * For well known types code may also use the pre-defined
     * runtime types that are fields of this class, such as INTEGER.
     * E.g. using DataTypeDescriptor.INTEGER is preferred to
     * DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER, true)
     * (both will return the same immutable object).
     *
     * @param jdbcType   The int type of the JDBC type for which to get
     *                   a corresponding SQL DataTypeDescriptor
     * @param isNullable TRUE means it could contain NULL, FALSE means
     *                   it definitely cannot contain NULL.
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     * A null return value means there is no corresponding SQL type
     */
    public static DataTypeDescriptor getBuiltInDataTypeDescriptor ( int jdbcType, boolean isNullable ){
        // Re-use pre-defined types wherever possible.
        switch(jdbcType){
            case Types.INTEGER:
                return isNullable?INTEGER:INTEGER_NOT_NULL;
            case Types.SMALLINT:
                return isNullable?SMALLINT:SMALLINT_NOT_NULL;
            case Types.BIGINT:
                return isNullable?LONGINT:LONGINT_NOT_NULL;
            default:
                break;
        }


        TypeId typeId=TypeId.getBuiltInTypeId(jdbcType);
        if(typeId==null){
            return null;
        }

        return new DataTypeDescriptor(typeId,isNullable);
    }

    /**
     * Get a descriptor that corresponds to a builtin JDBC type.
     * <p/>
     * Collation type will be UCS_BASIC and derivation IMPLICIT.
     *
     * @param jdbcType   The int type of the JDBC type for which to get
     *                   a corresponding SQL DataTypeDescriptor
     * @param isNullable TRUE means it could contain NULL, FALSE means
     *                   it definitely cannot contain NULL.
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     * A null return value means there is no corresponding SQL type
     */
    public static DataTypeDescriptor getBuiltInDataTypeDescriptor ( int jdbcType, boolean isNullable, int maxLength ){
        TypeId typeId=TypeId.getBuiltInTypeId(jdbcType);
        if(typeId==null){
            return null;
        }

        return new DataTypeDescriptor(typeId,isNullable,maxLength);
    }

    /**
     * Get a DataTypeServices that corresponds to a nullable builtin SQL type.
     * <p/>
     * Collation type will be UCS_BASIC and derivation IMPLICIT.
     *
     * @param sqlTypeName The name of the type for which to get
     *                    a corresponding SQL DataTypeDescriptor
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     * A null return value means there is no corresponding SQL type (only for 'char')
     */
    public static DataTypeDescriptor getBuiltInDataTypeDescriptor ( String sqlTypeName ){
        return new DataTypeDescriptor(TypeId.getBuiltInTypeId(sqlTypeName),true);
    }

    /**
     * Get a DataTypeServices that corresponds to a builtin SQL type
     * <p/>
     * Collation type will be UCS_BASIC and derivation IMPLICIT.
     *
     * @param sqlTypeName The name of the type for which to get
     *                    a corresponding SQL DataTypeDescriptor
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     * A null return value means there is no corresponding SQL type (only for 'char')
     */
    public static DataTypeDescriptor getBuiltInDataTypeDescriptor ( String sqlTypeName, int length ){
        return new DataTypeDescriptor(TypeId.getBuiltInTypeId(sqlTypeName),true,length);
    }

    /**
     * Get a DataTypeServices that corresponds to a Java type
     *
     * @param javaTypeName The name of the Java type for which to get
     *                     a corresponding SQL DataTypeDescriptor
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     * A null return value means there is no corresponding SQL type (only for 'char')
     */
    public static DataTypeDescriptor getSQLDataTypeDescriptor ( String javaTypeName ) throws StandardException{
        return DataTypeDescriptor.getSQLDataTypeDescriptor(javaTypeName,true);
    }

    /**
     * Get a DataTypeServices that corresponds to a Java type
     *
     * @param javaTypeName The name of the Java type for which to get
     *                     a corresponding SQL DataTypeDescriptor
     * @param isNullable   TRUE means it could contain NULL, FALSE means
     *                     it definitely cannot contain NULL.
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     * A null return value means there is no corresponding SQL type (only for 'char')
     */
    public static DataTypeDescriptor getSQLDataTypeDescriptor(String javaTypeName,
                                                              boolean isNullable) throws StandardException{
        TypeId typeId=TypeId.getSQLTypeForJavaType(javaTypeName);
        if(typeId==null){
            return null;
        }

        return new DataTypeDescriptor(typeId,isNullable);
    }

    /**
     * Get a DataTypeDescriptor that corresponds to a Java type
     *
     * @param javaTypeName The name of the Java type for which to get
     *                     a corresponding SQL DataTypeDescriptor
     * @param precision    The number of decimal digits
     * @param scale        The number of digits after the decimal point
     * @param isNullable   TRUE means it could contain NULL, FALSE means
     *                     it definitely cannot contain NULL.
     * @param maximumWidth The maximum width of a data value
     *                     represented by this type.
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     * A null return value means there is no corresponding SQL type.
     */
    public static DataTypeDescriptor getSQLDataTypeDescriptor(String javaTypeName,
                                                              int precision,
                                                              int scale,
                                                              boolean isNullable,
                                                              int maximumWidth ) throws StandardException{
        TypeId typeId=TypeId.getSQLTypeForJavaType(javaTypeName);
        if(typeId==null){
            return null;
        }

        return new DataTypeDescriptor(typeId, precision, scale, isNullable, maximumWidth);
    }

    /**
     * Get a catalog type that corresponds to a SQL Row Multiset
     *
     * @param columnNames  Names of the columns in the Row Muliset
     * @param catalogTypes Types of the columns in the Row Muliset
     * @return A new DataTypeDescriptor describing the SQL Row Multiset
     */
    public static TypeDescriptor getRowMultiSet(String[] columnNames, TypeDescriptor[] catalogTypes){
        RowMultiSetImpl rms=new RowMultiSetImpl(columnNames,catalogTypes);
        return new TypeDescriptorImpl(rms,true,-1);
    }

    /*
    ** Instance fields & methods
    */

    private TypeDescriptorImpl typeDescriptor;
    private TypeId typeId;
    private DataTypeDescriptor[] children;

    /**
     * Derivation of this type. All catalog types are
     * by definition implicit.
     */
    private int collationDerivation=StringDataValue.COLLATION_DERIVATION_IMPLICIT;


    /**
     * Public niladic constructor. Needed for Formatable interface to work.
     */
    public DataTypeDescriptor(){
    }

    /**
     * Constructor for use with numeric types
     *
     * @param typeId       The typeId of the type being described
     * @param precision    The number of decimal digits.
     * @param scale        The number of digits after the decimal point.
     * @param isNullable   TRUE means it could contain NULL, FALSE means
     *                     it definitely cannot contain NULL.
     * @param maximumWidth The maximum number of bytes for this datatype
     */
    public DataTypeDescriptor(TypeId typeId,int precision,int scale, boolean isNullable,int maximumWidth){
        this.typeId=typeId;
        typeDescriptor=new TypeDescriptorImpl(typeId.getBaseTypeId(),
                precision,
                scale,
                isNullable,
                maximumWidth);
    }

    /**
     * Constructor to use when the caller doesn't know if it is requesting
     * numeric or no-numeric DTD. For instance, when dealing with MAX/MIN
     * aggregrate operators, AggregateNode.bindExpression could be dealing
     * with a character string operand or a numeric operand. The result of
     * MAX/MIN will depend on the type of it's operand. And hence when this
     * constructor gets called by AggregateNode.bindExpression, we don't know
     * what type we are constructing and hence this constructor supports
     * arguments for both numeric and non-numeric types.
     *
     * @param typeId              The typeId of the type being described
     * @param precision           The number of decimal digits.
     * @param scale               The number of digits after the decimal point.
     * @param isNullable          TRUE means it could contain NULL, FALSE means
     *                            it definitely cannot contain NULL.
     * @param maximumWidth        The maximum number of bytes for this datatype
     * @param collationType       The collation type of a string data type
     * @param collationDerivation Collation Derivation of a string data type
     */
    public DataTypeDescriptor(TypeId typeId,
                              int precision,
                              int scale,
                              boolean isNullable,
                              int maximumWidth,
                              int collationType,
                              int collationDerivation){
        this.typeId=typeId;
        BaseTypeIdImpl baseTypeId=typeId.getBaseTypeId();
        typeDescriptor=new TypeDescriptorImpl(baseTypeId,precision,scale,isNullable,maximumWidth,collationType);
        this.collationDerivation=collationDerivation;
    }

    /**
     * Constructor for use with non-numeric types
     *
     * @param typeId       The typeId of the type being described
     * @param isNullable   TRUE means it could contain NULL, FALSE means
     *                     it definitely cannot contain NULL.
     * @param maximumWidth The maximum number of bytes for this datatype
     */
    public DataTypeDescriptor(TypeId typeId,boolean isNullable, int maximumWidth){
        this.typeId=typeId;
        typeDescriptor=new TypeDescriptorImpl(typeId.getBaseTypeId(), isNullable, maximumWidth);
    }


    public DataTypeDescriptor(TypeId typeId,boolean isNullable){
        this.typeId=typeId;
        typeDescriptor=new TypeDescriptorImpl(typeId.getBaseTypeId(),
                typeId.getMaximumPrecision(),
                typeId.getMaximumScale(),
                isNullable,
                typeId.getMaximumMaximumWidth());
    }

    private DataTypeDescriptor(DataTypeDescriptor source,boolean isNullable){
        //There might be other places, but one place this method gets called
        //from is ResultColumn.init. When the ResultColumn(RC) is for a
        //ColumnDescriptor(CD), the RC's TypeDescriptorImpl(TDI) should get
        //all the attributes of CD's TDI. So, if the CD is for a user table's
        //character type column, then this call by RC.init should have CD's
        //collation attributes copied into RC along with other attributes.
        this.typeId=source.typeId;
        typeDescriptor=new TypeDescriptorImpl(source.typeDescriptor,
                source.getPrecision(),
                source.getScale(),
                isNullable,
                source.getMaximumWidth(),
                source.getCollationType()
        );
        setChildren(source.getChildren());
        this.collationDerivation=source.getCollationDerivation();
    }

    private DataTypeDescriptor(DataTypeDescriptor source, int collationType, int collationDerivation){
        //There might be other places, but one place this method gets called
        //from is ResultColumn.init. When the ResultColumn(RC) is for a
        //ColumnDescriptor(CD), the RC's TypeDescriptorImpl(TDI) should get
        //all the attributes of CD's TDI. So, if the CD is for a user table's
        //character type column, then this call by RC.init should have CD's
        //collation attributes copied into RC along with other attributes.
        this.typeId=source.typeId;
        typeDescriptor=new TypeDescriptorImpl(source.typeDescriptor,
                source.getPrecision(),
                source.getScale(),
                source.isNullable(),
                source.getMaximumWidth(),
                collationType
        );
        setChildren(source.getChildren());
        this.collationDerivation=collationDerivation;
    }

    /**
     * Constructor for internal uses only.
     * (This is useful when the precision and scale are potentially wider than
     * those in the source, like when determining the dominant data type.)
     *
     * @param source       The DTSI to copy
     * @param precision    The number of decimal digits.
     * @param scale        The number of digits after the decimal point.
     * @param isNullable   TRUE means it could contain NULL, FALSE means
     *                     it definitely cannot contain NULL.
     * @param maximumWidth The maximum number of bytes for this datatype
     */
    public DataTypeDescriptor(DataTypeDescriptor source, int precision, int scale, boolean isNullable, int maximumWidth){
        this.typeId=source.typeId;
        typeDescriptor=new TypeDescriptorImpl(source.typeDescriptor,
                precision,
                scale,
                isNullable,
                maximumWidth,
                source.getCollationType()
        );
        setChildren(source.getChildren());
        this.collationDerivation=source.getCollationDerivation();
    }

    /**
     * Constructor for internal uses only
     *
     * @param source       The DTSI to copy
     * @param isNullable   TRUE means it could contain NULL, FALSE means
     *                     it definitely cannot contain NULL.
     * @param maximumWidth The maximum number of bytes for this datatype
     */
    public DataTypeDescriptor(DataTypeDescriptor source,boolean isNullable, int maximumWidth){
        this.typeId=source.typeId;
        typeDescriptor=new TypeDescriptorImpl(source.typeDescriptor,
                source.getPrecision(),
                source.getScale(),
                isNullable,
                maximumWidth,
                source.getCollationType()
        );
        setChildren(source.getChildren());
        this.collationDerivation=source.getCollationDerivation();

    }

    /**
     * Constructor for use in reconstructing a DataTypeDescriptor from a
     * TypeDescriptorImpl and a TypeId
     *
     * @param source The TypeDescriptorImpl to construct this DTSI from
     */
    private DataTypeDescriptor(TypeDescriptorImpl source,TypeId typeId){
        typeDescriptor=source;
        this.typeId=typeId;
    }

    /* DataTypeDescriptor Interface */
    public DataValueDescriptor normalize(DataValueDescriptor source,
                                         DataValueDescriptor cachedDest) throws StandardException{
        assert cachedDest!=null: "cachedDest is null";
        if(SanityManager.DEBUG){
            if(!getTypeId().isUserDefinedTypeId()){
                String t1=getTypeName();
                String t2=cachedDest.getTypeName();
                if(!t1.equals(t2)){

                    if(!(((t1.equals("DECIMAL") || t1.equals("NUMERIC"))
                            && (t2.equals("DECIMAL") || t2.equals("NUMERIC"))) ||
                            (t1.startsWith("INT") && t2.startsWith("INT"))))  //INT/INTEGER

                        SanityManager.THROWASSERT("Normalization of "+t2+" being asked to convert to "+t1);
                }
            }
        }

        if(source.isNull()){
            if(!isNullable())
                throw StandardException.newException(SQLState.LANG_NULL_INTO_NON_NULL,"");

            cachedDest.setToNull();
        }else{

            int jdbcId=getJDBCTypeId();

            cachedDest.normalize(this,source);
            //doing the following check after normalize so that normalize method would get called on long varchs and long varbinary
            //Need normalize to be called on long varchar for bug 5592 where we need to enforce a lenght limit in db2 mode
            if((jdbcId==Types.LONGVARCHAR) || (jdbcId==Types.LONGVARBINARY)){
                // special case for possible streams
                if(source.getClass()==cachedDest.getClass())
                    return source;
            }

        }
        return cachedDest;
    }

    /**
     * Get the dominant type (DataTypeDescriptor) of the 2.
     * For variable length types, the resulting type will have the
     * biggest max length of the 2.
     * If either side is nullable, then the result will also be nullable.
     * <p/>
     * If dealing with character string types, then make sure to set the
     * collation info on the dominant type. Following algorithm will be used
     * for dominant DTD's collation determination. Each of the steps of the
     * algorithm have been numbered in the comments below and those same
     * numbers are used in the actual algorithm below so it is easier to
     * understand and maintain.
     * <p/>
     * Step 1
     * If the DTD for "this" node has the same collation derivation as the
     * otherDTS, then check if their collation types match too. If the
     * collation types match too, then DTD for dominant type will get the same
     * collation derivation and type.
     * <p/>
     * Step 2
     * If the collation derivation for DTD for "this" node and otherDTS do not
     * match, then check if one of them has the collation derivation of NONE.
     * If that is the case, then dominant DTD will get the collation type and
     * derivation of DTD whose collation derivation is not NONE.
     * <p/>
     * Step 3
     * If the collation derivation for DTD for "this" node and otherDTS do not
     * match, and none of them have the derivation of NONE then it means that
     * we are dealing with collation derivation of IMPLICIT and EXPLICIT and
     * hence the dominant DTD should get collation derivation of NONE. This is
     * not a possibility in Derby 10.3 because the only 2 possible collation
     * derivation supported are IMPLICIT and NONE.
     * <p/>
     * Step 4
     * If the collation derivation for DTD for "this" node and otherDTS match,
     * then check if the collation types match too. If not, then the dominant
     * DTD should get collation derivation of NONE.
     *
     * @param otherDTS DataTypeDescriptor to compare with.
     * @param cf       A ClassFactory
     * @return DataTypeDescriptor  DTS for dominant type
     * @throws StandardException Thrown on error
     */
    public DataTypeDescriptor getDominantType(DataTypeDescriptor otherDTS,ClassFactory cf) throws StandardException{
        boolean nullable;
        TypeId thisType;
        TypeId otherType;
        DataTypeDescriptor higherType;
        DataTypeDescriptor lowerType = null;
        int maximumWidth;
        int precision=getPrecision();
        int scale=getScale();

        thisType=getTypeId();
        otherType=otherDTS.getTypeId();

        /* The result is nullable if either side is nullable */
        nullable=isNullable() || otherDTS.isNullable();

        /*
        ** The result will have the maximum width of both sides
        */
        maximumWidth=(getMaximumWidth()>otherDTS.getMaximumWidth())
                ?getMaximumWidth():otherDTS.getMaximumWidth();

        /* We need 2 separate methods of determining type dominance - 1 if both
         * types are system built-in types and the other if at least 1 is
         * a user type. (typePrecedence is meaningless for user types.)
         */
        if(!thisType.userType() && !otherType.userType()){
            TypeId higherTypeId;
            TypeId lowerTypeId;
            if(thisType.typePrecedence()>otherType.typePrecedence()){
                higherType=this;
                lowerType=otherDTS;
                higherTypeId=thisType;
                lowerTypeId=otherType;
            }else{
                higherType=otherDTS;
                lowerType=this;
                higherTypeId=otherType;
                lowerTypeId=thisType;
            }

            //Following is checking if higher type argument is real and other argument is decimal/bigint/integer/smallint,
            //then result type should be double
            if(higherTypeId.isRealTypeId() && (!lowerTypeId.isRealTypeId()) && lowerTypeId.isNumericTypeId()){
                higherType=DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE);
                higherTypeId=TypeId.getBuiltInTypeId(Types.DOUBLE);
            }
            /*
            ** If we have a DECIMAL/NUMERIC we have to do some
            ** extra work to make sure the resultant type can
            ** handle the maximum values for the two input
            ** types.  We cannot just take the maximum for
            ** precision.  E.g. we want something like:
            **
            **        DEC(10,10) and DEC(3,0) => DEC(13,10)
            **
            ** (var)char type needs some conversion handled later.
            */
            assert higherTypeId!=null;
            if(higherTypeId.isDecimalTypeId() && (!lowerTypeId.isStringTypeId())){
                precision=higherTypeId.getPrecision(this,otherDTS);
                if(precision> Limits.DB2_MAX_DECIMAL_PRECISION_SCALE)
                    precision=Limits.DB2_MAX_DECIMAL_PRECISION_SCALE; //db2 silently does this and so do we
                scale=higherTypeId.getScale(this,otherDTS);

                /* maximumWidth needs to count possible leading '-' and
                 * decimal point and leading '0' if scale > 0.  See also
                 * sqlgrammar.jj(exactNumericType).  Beetle 3875
                 */
                maximumWidth=DataTypeUtilities.computeMaxWidth(precision, scale);
            }else if(thisType.typePrecedence()!=otherType.typePrecedence()){
                precision=higherType.getPrecision();
                scale=higherType.getScale();

                /* GROSS HACKS:
                 * If we are doing an implicit (var)char->(var)bit conversion
                 * then the maximum width for the (var)char as a (var)bit
                 * is really 16 * its width as a (var)char.  Adjust
                 * maximumWidth accordingly.
                 * If we are doing an implicit (var)char->decimal conversion
                 * then we need to increment the decimal's precision by
                 * 2 * the maximum width for the (var)char and the scale
                 * by the maximum width for the (var)char. The maximumWidth
                 * becomes the new precision + 3.  This is because
                 * the (var)char could contain any decimal value from XXXXXX
                 * to 0.XXXXX.  (In other words, we don't know which side of the
                 * decimal point the characters will be on.)
                 */
                if(lowerTypeId.isStringTypeId()){
                    if(higherTypeId.isBitTypeId() &&
                            !(higherTypeId.isLongConcatableTypeId())){
                        if(lowerTypeId.isLongConcatableTypeId()){
                            if(maximumWidth>(Integer.MAX_VALUE/16))
                                maximumWidth=Integer.MAX_VALUE;
                            else
                                maximumWidth*=16;
                        }else{
                            int charMaxWidth;

                            int fromWidth=lowerType.getMaximumWidth();
                            if(fromWidth>(Integer.MAX_VALUE/16))
                                charMaxWidth=Integer.MAX_VALUE;
                            else
                                charMaxWidth=16*fromWidth;

                            maximumWidth=(maximumWidth>=charMaxWidth)?
                                    maximumWidth:charMaxWidth;
                        }
                    }
                }

                /*
                 * If we are doing an implicit (var)char->decimal conversion
                 * then the resulting decimal's precision could be as high as
                 * 2 * the maximum width (precisely 2mw-1) for the (var)char
                 * and the scale could be as high as the maximum width
                 * (precisely mw-1) for the (var)char.
                 * The maximumWidth becomes the new precision + 3.  This is
                 * because the (var)char could contain any decimal value from
                 * XXXXXX to 0.XXXXX.  (In other words, we don't know which
                 * side of the decimal point the characters will be on.)
                 *
                 * We don't follow this algorithm for long varchar because the
                 * maximum length of a long varchar is maxint, and we don't
                 * want to allocate a huge decimal value.  So in this case,
                 * the precision, scale, and maximum width all come from
                 * the decimal type.
                 */
                if(lowerTypeId.isStringTypeId()
                        && !(lowerTypeId.isLongConcatableTypeId())
                        && higherTypeId.isDecimalTypeId()){
                    int charMaxWidth=lowerType.getMaximumWidth();
                    int charPrecision;

                    /*
                    ** Be careful not to overflow when calculating the
                    ** precision.  Remember that we will be adding
                    ** three to the precision to get the maximum width.
                    */
                    if(charMaxWidth>(Integer.MAX_VALUE-3)/2)
                        charPrecision=Integer.MAX_VALUE-3;
                    else
                        charPrecision=charMaxWidth*2;

                    if(precision<charPrecision)
                        precision=charPrecision;

                    if(scale<charMaxWidth)
                        scale=charMaxWidth;

                    maximumWidth=precision+3;
                }
            } else { //if(thisType.typePrecedence()!=otherType.typePrecedence())
                // for char type of the two have different length, the resultant data type will become varchar
                // with length equals to the maximum among the two
                if (thisType == CHAR_ID && thisType == otherType && getMaximumWidth() != otherDTS.getMaximumWidth()) {
                    higherType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR);
                }
            }
        }else{
            /* At least 1 type is not a system built-in type */
            ClassInspector cu=cf.getClassInspector();

            if(cu.assignableTo(thisType.getCorrespondingJavaTypeName(),otherType.getCorrespondingJavaTypeName())){
                higherType=otherDTS;
            }else{
                if(SanityManager.DEBUG)
                    SanityManager.ASSERT(
                            cu.assignableTo(otherType.getCorrespondingJavaTypeName(),
                                    thisType.getCorrespondingJavaTypeName()),
                            otherType.getCorrespondingJavaTypeName()+
                                    " expected to be assignable to "+
                                    thisType.getCorrespondingJavaTypeName());

                higherType=this;
            }
            precision=higherType.getPrecision();
            scale=higherType.getScale();
        }

        if (higherType.getChildren() == null && lowerType.getChildren()!=null) // set children
            higherType.setChildren(lowerType.getChildren());

        higherType=new DataTypeDescriptor(higherType, precision,scale,nullable,maximumWidth);

        //Set collation info on the DTD for dominant type if it is string type
        //The algorithm used is explained in this method's javadoc
        if(higherType.getTypeId().isStringTypeId()){
            if(getCollationDerivation()!=otherDTS.getCollationDerivation()){
                if(getCollationDerivation()==StringDataValue.COLLATION_DERIVATION_NONE){
                    //Step 2
                    higherType=higherType.getCollatedType(
                            otherDTS.getCollationType(),
                            otherDTS.getCollationDerivation());

                }else if(otherDTS.getCollationDerivation()==StringDataValue.COLLATION_DERIVATION_NONE){
                    //Step 2
                    higherType=higherType.getCollatedType(
                            getCollationType(),
                            getCollationDerivation());
                }else{
                    //Step 3
                    higherType=higherType.getCollatedType(
                            StringDataValue.COLLATION_TYPE_UCS_BASIC, // ignored
                            StringDataValue.COLLATION_DERIVATION_NONE);
                }
            }else if(getCollationType()!=otherDTS.getCollationType())
                //Step 4
                higherType=higherType.getCollatedType(
                        StringDataValue.COLLATION_TYPE_UCS_BASIC, // ignored
                        StringDataValue.COLLATION_DERIVATION_NONE);
            else{
                //Step 1
                higherType=higherType.getCollatedType(
                        getCollationType(),
                        getCollationDerivation());
            }
        }

        return higherType;
    }

    /**
     * Check whether or not the 2 types (DataTypeDescriptor) have the same type
     * and length.
     * This is useful for UNION when trying to decide whether a NormalizeResultSet
     * is required.
     *
     * @param otherDTS DataTypeDescriptor to compare with.
     * @return boolean  Whether or not the 2 DTSs have the same type and length.
     */
    public boolean isExactTypeAndLengthMatch(DataTypeDescriptor otherDTS){
        /* Do both sides have the same length? */
        if(getMaximumWidth()!=otherDTS.getMaximumWidth()){
            return false;
        }
        if(getScale()!=otherDTS.getScale()){
            return false;
        }

        if(getPrecision()!=otherDTS.getPrecision()){
            return false;
        }

        TypeId thisType=getTypeId();
        TypeId otherType=otherDTS.getTypeId();

        /* Do both sides have the same type? */
        return thisType.equals(otherType);
    }

    /**
     * Shorthand method for getCatalogType().getMaximumWidth().
     *
     * @see TypeDescriptor#getMaximumWidth
     */
    public int getMaximumWidth(){
        return typeDescriptor.getMaximumWidth();
    }

    /**
     * Gets the TypeId for the datatype.
     *
     * @return The TypeId for the datatype.
     */
    public TypeId getTypeId(){
        return typeId;
    }

    /**
     * Get a Null for this type.
     */
    public DataValueDescriptor getNull() throws StandardException{
        DataValueDescriptor returnDVD=typeId.getNull();

        if (returnDVD instanceof SQLChar) {
            ((SQLChar)returnDVD).setSqlCharSize(typeDescriptor.getMaximumWidth());
        }

        //If we are dealing with default collation, then we have got the
        //right DVD already. Just return it.
        if (typeId.getTypeFormatId() == StoredFormatIds.DECIMAL_TYPE_ID) {
            ((SQLDecimal) returnDVD).setScale(typeDescriptor.getScale());
            ((SQLDecimal) returnDVD).setPrecision(typeDescriptor.getPrecision());
        }
        else if (typeId.getTypeFormatId() == StoredFormatIds.ARRAY_TYPE_ID) { // PopulateArrayType
            assert getCatalogType()!=null:"Catalog Type";
            assert ((TypeDescriptorImpl) getCatalogType()).getChildren() != null:"Catalog Type";
            TypeDescriptorImpl typeDescriptor = (TypeDescriptorImpl) ((TypeDescriptorImpl) getCatalogType()).getChildren()[0];
            ((SQLArray)returnDVD).setType(new DataTypeDescriptor(
                    TypeId.getBuiltInTypeId(typeDescriptor.getTypeId().getJDBCTypeId()),
                    true
            ).getNull());
            ((SQLArray)returnDVD).setBaseType(typeDescriptor.getTypeId().getJDBCTypeId());
            ((SQLArray)returnDVD).setBaseTypeName(typeDescriptor.getTypeId().getSQLTypeName());
        }
        else if(typeDescriptor.getCollationType()==StringDataValue.COLLATION_TYPE_UCS_BASIC)
            return returnDVD;
        //If we are dealing with territory based collation and returnDVD is
        //of type StringDataValue, then we need to return a StringDataValue
        //with territory based collation.
        if(returnDVD instanceof StringDataValue){
            try{
                RuleBasedCollator rbs=ConnectionUtil.getCurrentLCC().getDataValueFactory().
                        getCharacterCollator(typeDescriptor.getCollationType());
                return ((StringDataValue)returnDVD).getValue(rbs);
            }catch(java.sql.SQLException sqle){
                throw StandardException.plainWrapException(sqle);
            }
        }
        else
            return returnDVD;
    }

    /**
     * Gets the name of this datatype.
     *
     * @return the name of this datatype
     */
    public String getTypeName(){
        return typeDescriptor.getTypeName();
    }

    /**
     * Get the jdbc type id for this type.  JDBC type can be
     * found in java.sql.Types.
     * Shorthand method for getCatalogType().getJDBCTypeId().
     *
     * @return a jdbc type, e.g. java.sql.Types.DECIMAL
     * @see Types
     */
    public int getJDBCTypeId(){
        return typeDescriptor.getJDBCTypeId();
    }

    /**
     * Returns the number of decimal digits for the datatype, if applicable.
     * Shorthand method for getCatalogType().getPrecision().
     *
     * @return The number of decimal digits for the datatype.  Returns
     * zero for non-numeric datatypes.
     * @see TypeDescriptor#getPrecision()
     */
    public int getPrecision(){
        return typeDescriptor.getPrecision();
    }

    /**
     * Returns the number of digits to the right of the decimal for
     * the datatype, if applicable.
     * Shorthand method for getCatalogType().getScale().
     *
     * @return The number of digits to the right of the decimal for
     * the datatype.  Returns zero for non-numeric datatypes.
     * @see TypeDescriptor#getScale()
     */
    public int getScale(){
        return typeDescriptor.getScale();
    }

    /**
     * Obtain the collation type of the underlying catalog type.
     * Shorthand method for getCatalogType().getCollationType().
     *
     * @see TypeDescriptor#getCollationType()
     */
    public int getCollationType(){
        return typeDescriptor.getCollationType();
    }

    /**
     * Obtain the collation type from a collation property value.
     *
     * @return The collation type, or -1 if not recognized.
     */
    public static int getCollationType(String collationName){
        if(collationName.equalsIgnoreCase(Property.UCS_BASIC_COLLATION))
            return StringDataValue.COLLATION_TYPE_UCS_BASIC;
        else if(collationName.equalsIgnoreCase(Property.TERRITORY_BASED_COLLATION))
            return StringDataValue.COLLATION_TYPE_TERRITORY_BASED;
        else if(collationName.equalsIgnoreCase(Property.TERRITORY_BASED_PRIMARY_COLLATION))
            return StringDataValue.COLLATION_TYPE_TERRITORY_BASED_PRIMARY;
        else if(collationName.equalsIgnoreCase(Property.TERRITORY_BASED_SECONDARY_COLLATION))
            return StringDataValue.COLLATION_TYPE_TERRITORY_BASED_SECONDARY;
        else if(collationName.equalsIgnoreCase(Property.TERRITORY_BASED_TERTIARY_COLLATION))
            return StringDataValue.COLLATION_TYPE_TERRITORY_BASED_TERTIARY;
        else if(collationName.equalsIgnoreCase(Property.TERRITORY_BASED_IDENTICAL_COLLATION))
            return StringDataValue.COLLATION_TYPE_TERRITORY_BASED_IDENTICAL;
        else
            return -1;
    }

    /**
     * Gets the name of the collation type in this descriptor if the collation
     * derivation is not NONE. If the collation derivation is NONE, then this
     * method will return "NONE".
     * <p/>
     * This method is used for generating error messages which will use correct
     * string describing collation type/derivation.
     *
     * @return the name of the collation being used in this type.
     */
    public String getCollationName(){
        if(getCollationDerivation()==StringDataValue.COLLATION_DERIVATION_NONE)
            return Property.COLLATION_NONE;
        else
            return getCollationName(getCollationType());
    }

    /**
     * Gets the name of the specified collation type.
     *
     * @param collationType The collation type.
     * @return The name, e g "TERRITORY_BASED:PRIMARY".
     */
    public static String getCollationName(int collationType){
        switch(collationType){
            case StringDataValue.COLLATION_TYPE_TERRITORY_BASED:
                return Property.TERRITORY_BASED_COLLATION;
            case StringDataValue.COLLATION_TYPE_TERRITORY_BASED_PRIMARY:
                return Property.TERRITORY_BASED_PRIMARY_COLLATION;
            case StringDataValue.COLLATION_TYPE_TERRITORY_BASED_SECONDARY:
                return Property.TERRITORY_BASED_SECONDARY_COLLATION;
            case StringDataValue.COLLATION_TYPE_TERRITORY_BASED_TERTIARY:
                return Property.TERRITORY_BASED_TERTIARY_COLLATION;
            case StringDataValue.COLLATION_TYPE_TERRITORY_BASED_IDENTICAL:
                return Property.TERRITORY_BASED_IDENTICAL_COLLATION;
            default:
                return Property.UCS_BASIC_COLLATION;
        }
    }

    /**
     * Get the collation derivation for this type. This applies only for
     * character string types. For the other types, this api should be
     * ignored.
     * <p/>
     * SQL spec talks about character string types having collation type and
     * collation derivation associated with them (SQL spec Section 4.2.2
     * Comparison of character strings). If collation derivation says explicit
     * or implicit, then it means that there is a valid collation type
     * associated with the charcter string type. If the collation derivation is
     * none, then it means that collation type can't be established for the
     * character string type.
     * <p/>
     * 1)Collation derivation will be explicit if SQL COLLATE clause has been
     * used for character string type (this is not a possibility for Derby 10.3
     * because we are not planning to support SQL COLLATE clause in the 10.3
     * release).
     * <p/>
     * 2)Collation derivation will be implicit if the collation can be
     * determined w/o the COLLATE clause eg CREATE TABLE t1(c11 char(4)) then
     * c11 will have collation of USER character set. Another eg, TRIM(c11)
     * then the result character string of TRIM operation will have collation
     * of the operand, c11.
     * <p/>
     * 3)Collation derivation will be none if the aggregate methods are dealing
     * with character strings with different collations (Section 9.3 Data types
     * of results of aggregations Syntax Rule 3aii).
     * <p/>
     * Collation derivation will be initialized to COLLATION_DERIVATION_IMPLICIT
     * if not explicitly set.
     *
     * @return Should be COLLATION_DERIVATION_NONE or COLLATION_DERIVATION_IMPLICIT
     * @see StringDataValue#COLLATION_DERIVATION_NONE
     * @see StringDataValue#COLLATION_DERIVATION_IMPLICIT
     * @see StringDataValue#COLLATION_DERIVATION_EXPLICIT
     */
    public int getCollationDerivation(){
        return collationDerivation;
    }

    /**
     * Returns TRUE if the datatype can contain NULL, FALSE if not.
     * JDBC supports a return value meaning "nullability unknown" -
     * I assume we will never have columns where the nullability is unknown.
     * Shorthand method for getCatalogType().isNullable();
     *
     * @return TRUE if the datatype can contain NULL, FALSE if not.
     */
    public boolean isNullable(){
        return typeDescriptor.isNullable();
    }

    /**
     * Return a type descriptor identical to the this type
     * with the exception of its nullability. If the nullablity
     * required matches the nullability of this then this is returned.
     *
     * @param isNullable True to return a nullable type, false otherwise.
     */
    public DataTypeDescriptor getNullabilityType(boolean isNullable){
        if(isNullable()==isNullable)
            return this;

        return new DataTypeDescriptor(this,isNullable);
    }

    /**
     * Return a type description identical to this type
     * with the exception that its collation information is
     * taken from the passed in information. If the type
     * does not represent a string type then the collation
     * will be unchanged and this is returned.
     *
     * @return This if collation would be unchanged otherwise a new type.
     */
    public DataTypeDescriptor getCollatedType(int collationType, int collationDerivation){
        if(!typeDescriptor.isStringType())
            return this;

        if((getCollationType()==collationType) && (getCollationDerivation()==collationDerivation))
            return this;

        return new DataTypeDescriptor(this, collationType, collationDerivation);
    }

    /**
     * For a row multi set type return an identical type
     * with the collation type changed. Note that since
     * row types are only ever catalog types the
     * derivation is not used (since derivation is a property
     * of runtime types).
     * <BR>
     *
     * @param collationType the Collation type
     * @return this  will be returned if no changes are required (e.g.
     * no string types or collation is already correct), otherwise a
     * new instance is returned (leaving this unchanged).
     */
    private static TypeDescriptor getRowMultiSetCollation( TypeDescriptor catalogType,int collationType){
        TypeDescriptor[] rowTypes=catalogType.getRowTypes();

        TypeDescriptor[] newTypes=null;

        for(int t=0;t<rowTypes.length;t++){
            TypeDescriptor newType=DataTypeDescriptor.getCatalogType( rowTypes[t],collationType);

            // Is it the exact same as the old type.
            if(newType==rowTypes[t])
                continue;

            if(newTypes==null){
                // First different type, simply create a new
                // array and copy all the old types across.
                // Any new type will overwrite the old type.
                newTypes=new TypeDescriptor[rowTypes.length];
                System.arraycopy(rowTypes,0,newTypes,0,rowTypes.length);
            }

            newTypes[t]=newType;
        }

        // If no change then we continue to use this instance.
        if(newTypes==null)
            return catalogType;

        return DataTypeDescriptor.getRowMultiSet(
                catalogType.getRowColumnNames(),
                newTypes);
    }

    @Override
    public int hashCode() {
        return typeDescriptor.getTypeName().hashCode();
    }

    /**
     * Compare if two DataTypeDescriptors are exactly the same
     *
     * @param other the type to compare to.
     */
    public boolean equals(Object other){
        if(!(other instanceof DataTypeDescriptor))
            return false;

        DataTypeDescriptor odtd=(DataTypeDescriptor)other;
        return typeDescriptor.equals(odtd.typeDescriptor)
                && collationDerivation==odtd.collationDerivation;
    }

    /**
     * Check if this type is comparable with the passed type.
     *
     * @param compareWithDTD the type of the instance to compare with this type.
     * @return true if compareWithDTD is comparable to this type, else false.
     */


    public boolean comparable(DataTypeDescriptor compareWithDTD){

        TypeId compareWithTypeID=compareWithDTD.getTypeId();
        int compareWithJDBCTypeId=compareWithTypeID.getJDBCTypeId();

        // Long types cannot be compared.
        // XML types also fall in this window
        // Says SQL/XML[2003] spec:
        // 4.2.2 XML comparison and assignment
        // "XML values are not comparable."
        // An XML value cannot be compared to any type--
        // not even to other XML values.
        if(compareWithTypeID.isLongConcatableTypeId() || typeId.isLongConcatableTypeId())
            return false;

        // Ref types cannot be compared
        /*if (typeId.isRefTypeId() || compareWithTypeID.isRefTypeId())
            return false;
        */
        //If this DTD is not user defined type but the DTD to be compared with
        //is user defined type, then let the other DTD decide what should be the
        //outcome of the comparable method.
        if(!(typeId.isUserDefinedTypeId()) && (compareWithTypeID.isUserDefinedTypeId()))
            return compareWithDTD.comparable(this);

        //Numeric types are comparable to numeric types
        if(typeId.isNumericTypeId())
            return (compareWithTypeID.isNumericTypeId());

        //CHAR, VARCHAR and LONGVARCHAR are comparable to strings, boolean,
        //DATE/TIME/TIMESTAMP and to comparable user types
        if(typeId.isStringTypeId()){
            if((compareWithTypeID.isDateTimeTimeStampTypeID() || compareWithTypeID.isBooleanTypeId()))
                return true;
            //If both the types are string types, then we need to make sure
            //they have the same collation set on them
            return compareWithTypeID.isStringTypeId() && compareCollationInfo(compareWithDTD);
//can't be compared
        }

        //Are comparable to other bit types and comparable user types
        if(typeId.isBitTypeId())
            return (compareWithTypeID.isBitTypeId());

        //Booleans are comparable to Boolean, string, and to
        //comparable user types. As part of the work on DERYB-887,
        //I removed the comparability of booleans to numerics; I don't
        //understand the previous statement about comparable user types.
        //I suspect that is wrong and should be addressed when we
        //re-enable UDTs (see DERBY-651).
        if(typeId.isBooleanTypeId())
            return (compareWithTypeID.getSQLTypeName().equals(typeId.getSQLTypeName()) || compareWithTypeID.isStringTypeId());

        //Dates are comparable to dates, timestamps, strings and to comparable user types.
        if(typeId.getJDBCTypeId()==Types.DATE)
            return compareWithJDBCTypeId==Types.DATE
                    || compareWithJDBCTypeId==Types.TIMESTAMP
                    || compareWithTypeID.isStringTypeId();

        //Times are comparable to times, strings and to comparable
        //user types.
        if(typeId.getJDBCTypeId()==Types.TIME)
            return compareWithJDBCTypeId==Types.TIME || compareWithTypeID.isStringTypeId();

        //Timestamps are comparable to timestamps, dates, strings and to
        //comparable user types.
        if(typeId.getJDBCTypeId()==Types.TIMESTAMP)
            return compareWithJDBCTypeId==Types.TIMESTAMP
                    || compareWithJDBCTypeId==Types.DATE
                    || compareWithTypeID.isStringTypeId();

        if (typeId.getJDBCTypeId()==Types.ARRAY)
            return compareWithJDBCTypeId==Types.ARRAY;

        // Right now, user defined types are not comparable.
        // This removes old logic which we might want
        // to revive when we support comparable UDTs. See
        // DERBY-4470.
        return !(typeId.isUserDefinedTypeId() || typeId.getJDBCTypeId()==Types.OTHER)
                && typeId.getJDBCTypeId()==Types.REF
                && (compareWithTypeID.getJDBCTypeId()==Types.REF || compareWithTypeID.getJDBCTypeId()==Types.CHAR);

    }

    /**
     * Compare the collation info on this DTD with the passed DTD. The rules
     * for comparison are as follows (these are as per SQL standard 2003
     * Section 9.13)
     * <p/>
     * 1)If both the DTDs have collation derivation of NONE, then they can't be
     * compared and we return false.
     * 2)If both the DTDs have same collation derivation (which in Derby's case
     * at this point will mean collation derivation of IMPLICIT), then check
     * the collation types. If they match, then return true. If they do not
     * match, then they can't be compared and hence return false.
     * 3)If one DTD has collation derivation of IMPLICIT and other DTD has
     * collation derivation of NONE, then 2 DTDs are comparable using the
     * collation type of DTD with collation derivation of IMPLICIT. Derby does
     * not implement this rule currently and it is being traked as DERBY-2678.
     * Derby's current behavior is to throw an exception if both the DTDs
     * involved in collation operation do not have collation derivation of
     * IMPLICIT. This behavior is a subset of SQL standard.
     * 4)Derby currently does not support collation derivation of EXPLICIT and
     * hence we do not have the code to enforce rules as mentioned in Section
     * 9.13 of SQL spec for collation derivation of EXPLICIT. When we implement
     * collation derivation of EXPLICIT, we should make sure that we follow the
     * rules as specified in the SQL spec for comparability.
     *
     * @param compareWithDTD compare this DTD's collation info
     * @return value depends on the algorithm above.
     */
    public boolean compareCollationInfo(DataTypeDescriptor compareWithDTD){
        //both the operands can not have the collation derivation of
        //NONE. This is because in that case, we do not know what kind
        //of collation to use for comparison.
        if(getCollationDerivation()==compareWithDTD.getCollationDerivation() &&
                getCollationDerivation()==StringDataValue.COLLATION_DERIVATION_NONE)
            return false;
        //collation matches
        return getCollationDerivation()==compareWithDTD.getCollationDerivation()
                && getCollationType()==compareWithDTD.getCollationType();
    }

    /**
     * Converts this data type descriptor (including length/precision)
     * to a string. E.g.
     * <p/>
     * VARCHAR(30)
     * <p/>
     * or
     * <p/>
     * java.util.Hashtable
     *
     * @return String version of datatype, suitable for running through
     * the Parser.
     */
    public String getSQLstring(){
        return typeId.toParsableString(this);
    }

    /**
     * Get the simplified type descriptor that is intended to be stored
     * in the system tables.
     */
    public TypeDescriptor getCatalogType(){
        return typeDescriptor;
    }

    /**
     * Get the estimated memory usage for this type descriptor.
     */
    public double estimatedMemoryUsage(){
        switch(typeId.getTypeFormatId()){
            case StoredFormatIds.LONGVARBIT_TYPE_ID:
                /* Who knows?  Let's just use some big number */
                return 10000.0;

            case StoredFormatIds.BIT_TYPE_ID:
                return (((float)getMaximumWidth())/8.0)+0.5;

            case StoredFormatIds.BOOLEAN_TYPE_ID:
                return 4.0;

            case StoredFormatIds.CHAR_TYPE_ID:
            case StoredFormatIds.VARCHAR_TYPE_ID:
                return 2.0*getMaximumWidth();

            case StoredFormatIds.LONGVARCHAR_TYPE_ID:
                /* Who knows? Let's just use some big number */
                return 10000.0;

            case StoredFormatIds.DECIMAL_TYPE_ID:
                /*
                ** 0.415 converts from number decimal digits to number of 8-bit digits.
                ** Add 1.0 for the sign byte, and 0.5 to force it to round up.
                */
                return (getPrecision()*0.415)+1.5;

            case StoredFormatIds.DOUBLE_TYPE_ID:
                return 8.0;

            case StoredFormatIds.INT_TYPE_ID:
                return 4.0;

            case StoredFormatIds.LONGINT_TYPE_ID:
                return 8.0;

            case StoredFormatIds.REAL_TYPE_ID:
                return 4.0;

            case StoredFormatIds.SMALLINT_TYPE_ID:
                return 2.0;

            case StoredFormatIds.TINYINT_TYPE_ID:
                return 1.0;

            case StoredFormatIds.REF_TYPE_ID:
                /* I think 12 is the right number */
                return 12.0;

            case StoredFormatIds.USERDEFINED_TYPE_ID_V3:
                if(typeId.userType()){
                    /* Who knows?  Let's just use some medium-sized number */
                    return 256.0;
                }
            case StoredFormatIds.DATE_TYPE_ID:
            case StoredFormatIds.TIME_TYPE_ID:
            case StoredFormatIds.TIMESTAMP_TYPE_ID:
                return 12.0;

            default:
                return 0.0;
        }
    }

    /**
     * Compare JdbcTypeIds to determine if they represent equivalent
     * SQL types. For example Types.NUMERIC and Types.DECIMAL are
     * equivalent
     *
     * @param existingType JDBC type id of Derby data type
     * @param jdbcTypeId   JDBC type id passed in from application.
     * @return boolean true if types are equivalent, false if not
     */

    public static boolean isJDBCTypeEquivalent(int existingType,int jdbcTypeId){
        // Any type matches itself.
        if(existingType==jdbcTypeId)
            return true;

        // To a numeric type
        if(DataTypeDescriptor.isNumericType(existingType)){
            return DataTypeDescriptor.isNumericType(jdbcTypeId) || DataTypeDescriptor.isCharacterType(jdbcTypeId);
        }

        // To character type.
        if(DataTypeDescriptor.isCharacterType(existingType)){

            if(DataTypeDescriptor.isCharacterType(jdbcTypeId))
                return true;

            if(DataTypeDescriptor.isNumericType(jdbcTypeId))
                return true;


            switch(jdbcTypeId){
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    return true;
                default:
                    break;
            }


            return false;

        }

        // To binary type
        if(DataTypeDescriptor.isBinaryType(existingType)){
            return DataTypeDescriptor.isBinaryType(jdbcTypeId);
        }

        // To DATE, TIME
        if(existingType==Types.DATE || existingType==Types.TIME){
            return DataTypeDescriptor.isCharacterType(jdbcTypeId) || jdbcTypeId==Types.TIMESTAMP;
        }

        // To TIMESTAMP
        if(existingType==Types.TIMESTAMP){
            return DataTypeDescriptor.isCharacterType(jdbcTypeId) || jdbcTypeId==Types.DATE;
        }

        // To CLOB
        return existingType==Types.CLOB && DataTypeDescriptor.isCharacterType(jdbcTypeId);
    }

    public static boolean isNumericType(int jdbcType){

        switch(jdbcType){
            case Types.BIT:
            case Types.BOOLEAN:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.DECIMAL:
            case Types.NUMERIC:
                return true;
            default:
                return false;
        }
    }

    /**
     * Check whether a JDBC type is one of the character types that are
     * compatible with the Java type <code>String</code>.
     * <p/>
     * <p><strong>Note:</strong> <code>CLOB</code> is not compatible with
     * <code>String</code>. See tables B-4, B-5 and B-6 in the JDBC 3.0
     * Specification.
     * <p/>
     * <p> There are some non-character types that are compatible with
     * <code>String</code> (examples: numeric types, binary types and
     * time-related types), but they are not covered by this method.
     *
     * @param jdbcType a JDBC type
     * @return <code>true</code> iff <code>jdbcType</code> is a character type
     * and compatible with <code>String</code>
     * @see java.sql.Types
     */
    private static boolean isCharacterType(int jdbcType){

        switch(jdbcType){
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return true;
            default:
                return false;
        }
    }

    /**
     * Check whether a JDBC type is compatible with the Java type
     * <code>byte[]</code>.
     * <p/>
     * <p><strong>Note:</strong> <code>BLOB</code> is not compatible with
     * <code>byte[]</code>. See tables B-4, B-5 and B-6 in the JDBC 3.0
     * Specification.
     *
     * @param jdbcType a JDBC type
     * @return <code>true</code> iff <code>jdbcType</code> is compatible with
     * <code>byte[]</code>
     * @see java.sql.Types
     */
    private static boolean isBinaryType(int jdbcType){
        switch(jdbcType){
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return true;
            default:
                return false;
        }
    }

    /**
     * Determine if an ASCII stream can be inserted into a column or parameter
     * of type <code>jdbcType</code>.
     *
     * @param jdbcType JDBC type of column or parameter
     * @return <code>true</code> if an ASCII stream can be inserted;
     * <code>false</code> otherwise
     */
    public static boolean isAsciiStreamAssignable(int jdbcType){
        return jdbcType==Types.CLOB || isCharacterType(jdbcType);
    }

    /**
     * Determine if a binary stream can be inserted into a column or parameter
     * of type <code>jdbcType</code>.
     *
     * @param jdbcType JDBC type of column or parameter
     * @return <code>true</code> if a binary stream can be inserted;
     * <code>false</code> otherwise
     */
    public static boolean isBinaryStreamAssignable(int jdbcType){
        return jdbcType==Types.BLOB || isBinaryType(jdbcType);
    }

    /**
     * Determine if a character stream can be inserted into a column or
     * parameter of type <code>jdbcType</code>.
     *
     * @param jdbcType JDBC type of column or parameter
     * @return <code>true</code> if a character stream can be inserted;
     * <code>false</code> otherwise
     */
    public static boolean isCharacterStreamAssignable(int jdbcType){
        // currently, we support the same types for ASCII streams and
        // character streams
        return isAsciiStreamAssignable(jdbcType);
    }

    public String toString(){
        return typeDescriptor.toString();
    }

    public String toSparkString() {
        if (typeDescriptor.getJDBCTypeId() == Types.NUMERIC) {
            String typeString = typeDescriptor.getSQLstring();
            String pattern = "(NUMERIC)";
            typeString = typeString.replaceFirst(pattern, "DECIMAL");
            return typeString;
        }
        return typeDescriptor.getSQLstring();
    }

    // Formatable methods

    /**
     * Read this object from a stream of stored objects.
     *
     * @param in read this.
     * @throws IOException            thrown on error
     * @throws ClassNotFoundException thrown on error
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        typeDescriptor=(TypeDescriptorImpl)in.readObject();
        collationDerivation=in.readInt();

        String typeName=this.getTypeName();
        typeId=TypeId.getBuiltInTypeId(typeName);
        if(typeId==null && typeName!=null){
         /*
          * This is a User-defined TypeId, which we serialize. For whatever reason,
          * derby does not approve of serializing the TypeId information for user-defined
          * types, so we have to make do with this instead
          */
            try{
                typeId=TypeId.getUserDefinedTypeId(typeName,false);
            }catch(StandardException se){
                throw new IOException(se);
            }
        }
    }

    /**
     * Write this object to a stream of stored objects.
     *
     * @param out write bytes here.
     * @throws IOException thrown on error
     */
    public void writeExternal(ObjectOutput out) throws IOException{
        out.writeObject(typeDescriptor);
        out.writeInt(getCollationDerivation());
    }

    /**
     * Get the formatID which corresponds to this class.
     *
     * @return the formatID of this class
     */
    public int getTypeFormatId(){
        return StoredFormatIds.DATA_TYPE_DESCRIPTOR_V02_ID;
    }

    /**
     * Check to make sure that this type id is something a user can create
     * him/herself directly through an SQL CREATE TABLE statement.
     * <p/>
     * This method is used for CREATE TABLE AS ... WITH [NO] DATA binding
     * because it's possible for the query to return types which are not
     * actually creatable for a user.  DERBY-2605.
     * <p/>
     * Three examples are:
     * <p/>
     * JAVA_OBJECT: A user can select columns of various java object types
     * from system tables, but s/he is not allowed to create such a column
     * him/herself.
     * <p/>
     * DECIMAL: A user can specify a VALUES clause with a constant that
     * has a precision of greater than 38.  Derby can apparently handle
     * such a value internally, but the user is not supposed to be able
     * create such a column him/herself.
     *
     * @return True if the type associated with this DTD can be created via
     * the CREATE TABLE syntax; false otherwise.
     */
    public boolean isUserCreatableType() throws StandardException{
        switch(typeId.getJDBCTypeId()){
            case Types.JAVA_OBJECT:
                return getTypeId().getBaseTypeId().isAnsiUDT();
            case Types.DECIMAL:
                return
                        (getPrecision()<=typeId.getMaximumPrecision()) && (getScale()<=typeId.getMaximumScale());
            default:
                break;
        }
        return true;
    }

    /**
     * Return the SQL type name and, if applicable, scale/precision/length
     * for this DataTypeDescriptor.  Note that we want the values from *this*
     * object specifically, not the max values defined on this.typeId.
     */
    public String getFullSQLTypeName(){
        StringBuilder sbuf=new StringBuilder(typeId.getSQLTypeName());
        if(typeId.isDecimalTypeId() || typeId.isNumericTypeId()){
            sbuf.append("(");
            sbuf.append(getPrecision());
            sbuf.append(", ");
            sbuf.append(getScale());
            sbuf.append(")");
        }else if(typeId.variableLength()){
            sbuf.append("(");
            sbuf.append(getMaximumWidth());
            sbuf.append(")");
        }

        return sbuf.toString();
    }

    /* Return the typename with the collation name for
     * String types.
     */
    public String getSQLTypeNameWithCollation(){
        String name=typeId.getSQLTypeName();
        if(typeId.isStringTypeId()){
            name=name+" ("+getCollationName()+")";
        }
        return name;
    }

    public DataTypeDescriptor[] getChildren() {
        return this.children;
    }

    public void setChildren(DataTypeDescriptor[] children) {
        this.children = children;
        if (children != null) {
            TypeDescriptor[] array = new TypeDescriptor[children.length];
            for (int i = 0; i< children.length; i++) {
                array[i] = children[i].typeDescriptor;
            }
            typeDescriptor.setChildren(array);
         }
     }

    public DataValueDescriptor getDefault() throws StandardException {
        if (typeId.getTypeFormatId() == StoredFormatIds.BIT_TYPE_ID) {
            return new SQLBit(new byte[typeDescriptor.getMaximumWidth()]);
        }

        return typeId.getDefault();
    }

    public StructField getStructField(String columnName) {
        StructField childStructField = null;
        if (typeId.isArray()) {
            // get the child type descriptor & struct field
            assert typeDescriptor.getChildren() != null:"ArrayType should have the child type specified";
            TypeDescriptorImpl childTypeDescriptor = (TypeDescriptorImpl) typeDescriptor.getChildren()[0];
            TypeId childTypeId = TypeId.getBuiltInTypeId(childTypeDescriptor.getTypeId().getJDBCTypeId());
            childStructField = childTypeId.getStructField("co", childTypeDescriptor.getPrecision(), childTypeDescriptor.getScale(), null);

        }
        return typeId.getStructField(columnName, getPrecision(), getScale(), childStructField);

    }
}

