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

//depot/main/java/com.splicemachine.db.iapi.services.io/StoredFormatIds.java#211 - edit change 20974 (text)
package com.splicemachine.db.iapi.services.io;
/**
  A format id identifies a stored form of an object for the
  purposes of locating a class which can read the stored form and
  reconstruct the object using the java.io.Externalizable interface.

  <P>An important aspect of the format id concept is that it does
  not impose an implementation on the stored object. Rather,
  multiple implementations of an object (or interface) may share a
  format id. One implementation may store (write) an object
  and another may restore (read) the object. The implication of this
  is that a format id specifies the following properties of a
  stored object.

  <UL>
  <LI>The interface(s) the stored object must support. Any implementation
  which reads the object must support these interfaces.
  <LI>The format of the stored object. All implementaions which support
  the format must be able to read and write it.
  </UL>

  <P>An object should implement the Formatable inteface to support a
  stored format. In addition, the module which contains the object
  should register the object's class with the Monitor (See
  FormatIdUtil.register.)

  <P>When you add a format id to this file, please include the list
  of interfaces an implementaion must support when it supports
  the format id. When Derby code reads a stored form it returns an 
  object of a Class which supports the stored form. A reader may
  cast this object to any interface listed in this file. It is an error for
  the reader to cast the object to a class or interface not listed in this
  file.

  <P>When you implement a class that supports a format, add a comment that
  states the name of the class. The first implementation of a format defines
  the stored form.

  <P>This interface defines all the format ids for Derby.
  If you define a format id please be sure to declare it in this
  file. If you remove support for a one please document that the
  format id is deprecated. Never remove or re-use a format id.
 */
public interface StoredFormatIds {

    /** Byte length of a two byt format id. */
    int  TWO_BYTE_FORMAT_ID_BYTE_LENGTH = 2;

    /** Minimum value for a two byte format id. */
    int  MIN_TWO_BYTE_FORMAT_ID = 0; //16384
    /** Maximum value for a two byte format id. */
    int  MAX_TWO_BYTE_FORMAT_ID = 0x7FFF; //32767
    
    int MIN_ID_2 = MIN_TWO_BYTE_FORMAT_ID;

    // TEMP DJD
    int MIN_ID_4 = MIN_ID_2 + 403;

    /******************************************************************
    **
    **      How to add an ID for another Formatable class 
    **
    **      o       In the list of constants below, identify the module that
    **              defines your class.
    **
    **      o       Add your class to the list to the end of that module 
    **              use a number that is one greater than all existing formats
    **              in that module, see MAX_ID_2 or MAX_ID_4 at the end of the 
    **              file, these are the largest existing formatId.
    **
    **      o       update MAX_ID_2 and MAX_ID_4
    **
    **
    **      o       Make sure that the getFormatId() method for your class
    **              returns the constant that you just made up.
    **
    **      o       Now find your module startup code that registers Format
    **              IDs. Add your class to that list.
    **
    **      o   Add a test for your new format ID to T_StoredFormat.java
    **
    ******************************************************************/


    /******************************************************************
    **
    **      Formats for the StoredFormatModule
    **
    **
    **
    ******************************************************************/

    /** Special format id for any null referance */
    int NULL_FORMAT_ID =
            (MIN_ID_2 + 0);

    /** Special format id for tagging UTF8 strings */
    int STRING_FORMAT_ID =
            (MIN_ID_2 + 1);

    /** Special format id for tagging Serializable objects. */
    int SERIALIZABLE_FORMAT_ID =
            (MIN_ID_2 + 2);
    
    /******************************************************************
    **
    **      DataDictionary Formats
    **
    **
    **
    ******************************************************************/
    /**
        class com.splicemachine.db.iapi.types.BooleanTypeId
     */
    int BOOLEAN_TYPE_ID =
            (MIN_ID_2 + 4);
    
    /**
        class com.splicemachine.db.iapi.types.BooleanTypeId
     */
    int BOOLEAN_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 260);
    
    /**
        class com.splicemachine.db.iapi.types.CharTypeId
     */
    int CHAR_TYPE_ID =
            (MIN_ID_2 + 5);

    /**
        class com.splicemachine.db.iapi.types.CharTypeId
     */
    int CHAR_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 244);

    /**
        class com.splicemachine.db.iapi.types.DoubleTypeId
     */
    int DOUBLE_TYPE_ID =
            (MIN_ID_2 + 6);
    
    /**
        class com.splicemachine.db.iapi.types.DoubleTypeId
     */
    int DOUBLE_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 245);
    
    /**
        class com.splicemachine.db.iapi.types.IntTypeId
     */
    int INT_TYPE_ID =
            (MIN_ID_2 + 7);

    /**
        class com.splicemachine.db.iapi.types.IntTypeId
     */
    int INT_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 246);

    /**
        class com.splicemachine.db.iapi.types.RealTypeId
     */
    int REAL_TYPE_ID =
            (MIN_ID_2 + 8);

    /**
        class com.splicemachine.db.iapi.types.RealTypeId
     */
    int REAL_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 247);

    /**
        class com.splicemachine.db.iapi.types.RefTypeId
     */
    int REF_TYPE_ID =
            (MIN_ID_2 + 9);

    /**
        class com.splicemachine.db.iapi.types.RefTypeId
     */
    int REF_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 248);
    
    /**
        class com.splicemachine.db.iapi.types.SmallintTypeId
     */
    int SMALLINT_TYPE_ID =
            (MIN_ID_2 + 10);
    
    /**
        class com.splicemachine.db.iapi.types.SmallintTypeId
     */
    int SMALLINT_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 249);
    
    /**
        class com.splicemachine.db.iapi.types.LongintTypeId
     */
    int LONGINT_TYPE_ID =
            (MIN_ID_2 + 11);
    
    /**
        class com.splicemachine.db.iapi.types.LongintTypeId
     */
    int LONGINT_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 250);
    
    /**
        class com.splicemachine.db.iapi.types.UserDefinedTypeId
     */
    //static public final int USERDEFINED_TYPE_ID =
    //      (MIN_ID_2 + 12);
    
    /**
        class com.splicemachine.db.iapi.types.UserDefinedTypeIdV2
     */
    //static public final int USERDEFINED_TYPE_ID_V2 =
    //      (MIN_ID_2 + 267);
    /**
        class com.splicemachine.db.iapi.types.UserDefinedTypeIdV3
     */
    int USERDEFINED_TYPE_ID_V3 =
            (MIN_ID_2 + 267);
    
    /**
        class com.splicemachine.db.iapi.types.UserDefinedTypeId
     */
    int USERDEFINED_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 251);
    
    /**
        class com.splicemachine.db.iapi.types.UserDefinedTypeIdV2
     */
    int USERDEFINED_COMPILATION_TYPE_ID_V2 =
            (MIN_ID_2 + 265);
    
    /**
        class com.splicemachine.db.iapi.types.VarcharTypeId
     */
    int VARCHAR_TYPE_ID =
            (MIN_ID_2 + 13);
    
    /**
        class com.splicemachine.db.iapi.types.VarcharTypeId
     */
    int VARCHAR_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 252);
    
    /**
    class com.splicemachine.db.catalog.types.TypeDescriptorImpl
    */
    int DATA_TYPE_IMPL_DESCRIPTOR_V01_ID =
            (MIN_ID_2 + 14);
    
    /**
     * In releases prior to 10.3 this format was produced by
     * DataTypeDescriptor. The format was incorrect used
     * in system catalogs for routine parameter and return
     * types. The format contained repeated information.
     * DERBY-2775 changed the code so that these catalog
     * types were written as TypeDescriptor (which is what
     * always had occurred for the types in SYSCOLUMNS).
     * <P>
     * This format now maps to OldRoutineType and is solely
     * used to read old routine types.
     */
    int DATA_TYPE_SERVICES_IMPL_V01_ID =
            (MIN_ID_2 + 259);

    /**
    class com.splicemachine.db.impl.sql.catalog.ConglomerateDescriptorFinder
     */
    int CONGLOMERATE_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 135);
    
    /**
    class com.splicemachine.db.impl.sql.catalog.ConstraintDescriptorFinder
     */
    int CONSTRAINT_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 208);
    
    /**
    class com.splicemachine.db.impl.sql.catalog.DefaultDescriptorFinder
     */
    int DEFAULT_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 325);
    
    /**
    class com.splicemachine.db.impl.sql.catalog.AliasDescriptorFinder
     */
    int ALIAS_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 136);

    /**
    class com.splicemachine.db.impl.sql.catalog.TableDescriptorFinder
     */
    int TABLE_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 137);

    /**
    class com.splicemachine.db.impl.sql.catalog.CoreDDFinderClassInfo
     */
    int ROUTINE_PERMISSION_FINDER_V01_ID =
            (MIN_ID_2 + 461);

    /**
    class com.splicemachine.db.impl.sql.catalog.CoreDDFinderClassInfo
     */
    int TABLE_PERMISSION_FINDER_V01_ID =
            (MIN_ID_2 + 462);

    /**
    class com.splicemachine.db.impl.sql.catalog.CoreDDFinderClassInfo
     */
    int COLUMNS_PERMISSION_FINDER_V01_ID =
            (MIN_ID_2 + 463);

    /**
     class com.splicemachine.db.impl.sql.catalog.CoreDDFinderClassInfo
     */
    int SCHEMA_PERMISSION_FINDER_V01_ID =
            (MIN_ID_2 + 464);

    /**
    class com.splicemachine.db.impl.sql.catalog.CoreDDFinderClassInfo
     */
    int ROLE_GRANT_FINDER_V01_ID =
            (MIN_ID_2 + 471);
    
    /**
    class com.splicemachine.db.impl.sql.catalog.DataDictionaryDescriptorFinder
     */
    int DATA_DICTIONARY_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 138);
    
    /**
    class com.splicemachine.db.impl.sql.catalog.ViewDescriptorFinder
     */
    int VIEW_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 145);
    
    /**
    class com.splicemachine.db.impl.sql.catalog.SPSDescriptorFinder
     */
    int SPS_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 226);

    /**
    class com.splicemachine.db.impl.sql.catalog.FileInfoFinder
     */
    int FILE_INFO_FINDER_V01_ID =
            (MIN_ID_2 + 273);

    /**
    class com.splicemachine.db.impl.sql.catalog.TriggerDescriptorFinder
     */
    int TRIGGER_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 320);

    /**
    class com.splicemachine.db.impl.sql.catalog.TriggerDescriptorFinder
     */
    int TRIGGER_DESCRIPTOR_V01_ID =
            (MIN_ID_2 + 316);

    /**
    class com.splicemachine.db.impl.sql.catalog.DD_SocratesVersion
     */
    int DD_SOCRATES_VERSION_ID =
            (MIN_ID_2 + 174);
    
    /**
    class com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl
     */
    int REFERENCED_COLUMNS_DESCRIPTOR_IMPL_V01_ID =
            (MIN_ID_2 + 205);
    
    /**
    class com.splicemachine.db.impl.sql.catalog.DD_PlatoVersion
     */
    int DD_PLATO_VERSION_ID =
            (MIN_ID_2 + 206);

    /**
    class com.splicemachine.db.impl.sql.catalog.DD_AristotleVersion
     */
    int DD_ARISTOTLE_VERSION_ID =
            (MIN_ID_2 + 272);

    /**
      class com.splicemachine.db.impl.sql.catalog.DD_XenaVersion
     */
    int DD_XENA_VERSION_ID =
            (MIN_ID_2 + 302);

    /**
      class com.splicemachine.db.impl.sql.catalog.DD_BuffyVersion
     */
    int DD_BUFFY_VERSION_ID =
            (MIN_ID_2 + 373);

    /**
      class com.splicemachine.db.impl.sql.catalog.DD_MulanVersion
     */
    int DD_MULAN_VERSION_ID =
            (MIN_ID_2 + 376);

    /**
      class com.splicemachine.db.impl.sql.catalog.DD_IvanovaVersion
     */
    int DD_IVANOVA_VERSION_ID =
            (MIN_ID_2 + 396);

    /**
      class com.splicemachine.db.impl.sql.catalog.DD_DB2J72
      now mapped to a single class DD_Version.
      5.0 databases will have this as the format identifier for their
      catalog version number.
     */
    int DD_DB2J72_VERSION_ID =
            (MIN_ID_2 + 401);

    /**
      class com.splicemachine.db.impl.sql.catalog.DD_Version
      now mapped to a single class DD_Version.
      5.1 and later databases will have this as the format identifier for their
      catalog version number.
    */
    int DD_ARWEN_VERSION_ID =
            (MIN_ID_2 + 402);
    
    /**
            class com.splicemachine.db.iapi.types.LongvarcharTypeId
     */
    int LONGVARCHAR_TYPE_ID =
            (MIN_ID_2 + 230);
    
    /**
            class com.splicemachine.db.iapi.types.LongvarcharTypeId
     */
    int LONGVARCHAR_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 256);

    /**
            class com.splicemachine.db.iapi.types.LongvarcharTypeId
     */
    int CLOB_TYPE_ID =
            (MIN_ID_2 + 444);
    
    /**
            class com.splicemachine.db.iapi.types.LongvarcharTypeId
     */
    int CLOB_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 445);

    /**
            class com.splicemachine.db.iapi.types.LongvarbitTypeId
            - XXXX does not exist!!!
     */
    int LONGVARBIT_TYPE_ID =
            (MIN_ID_2 + 232);

    /**
            class com.splicemachine.db.iapi.types.LongvarbitTypeId
            - XXXX does not exist!!!
     */
    int LONGVARBIT_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 255);

    /**
            class com.splicemachine.db.iapi.types.LongvarbitTypeId
            - XXXX does not exist!!!
    But for BLOB we do the same as for LONGVARBIT, only need different ids
     */
    int BLOB_TYPE_ID =
            (MIN_ID_2 + 440);

    /**
            class com.splicemachine.db.iapi.types.LongvarbitTypeId
            - XXXX does not exist!!!
    But for BLOB we do the same as for LONGVARBIT, only need different ids
     */
    int BLOB_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 441);

    /**
            Instance of TypeId for XML data types.
     */
    int XML_TYPE_ID =
            (MIN_ID_2 + 456);
    
    /**
        class com.splicemachine.db.iapi.types.JSQLType
     */
    int JSQLTYPEIMPL_ID =
            (MIN_ID_2 + 307);

    /**
    class com.splicemachine.db.impl.sql.catalog.RowListImpl
     */
    int ROW_LIST_V01_ID =
            (MIN_ID_2 + 239);
    
    /**
     * DataTypeDescriptor (runtime type) new format from 10.4
     * onwards that reflects the change in role from is a TypeDescriptor
     * to has a TypeDescriptor. Fixes the format so that information
     * is not duplicated.
     * Old format number was DATA_TYPE_SERVICES_IMPL_V01_ID (259).
     */
    int DATA_TYPE_DESCRIPTOR_V02_ID =
            (MIN_ID_2 + 240);

    /**
    class com.splicemachine.db.impl.sql.catalog.IndexRowGeneratorImpl
     */
    int INDEX_ROW_GENERATOR_V01_ID =
            (MIN_ID_2 + 268);

    /**
    class com.splicemachine.db.iapi.services.io.FormatableBitSet
     */
    int BITIMPL_V01_ID =
            (MIN_ID_2 + 269);

    /**
    class com.splicemachine.db.iapi.services.io.FormatableArrayHolder
     */
    int FORMATABLE_ARRAY_HOLDER_V01_ID =
            (MIN_ID_2 + 270);
    
    /**
    class com.splicemachine.db.iapi.services.io.FormatableProperties
     */
    int FORMATABLE_PROPERTIES_V01_ID =
            (MIN_ID_2 + 271);

    /**
    class com.splicemachine.db.iapi.services.io.FormatableIntHolder
     */
    int FORMATABLE_INT_HOLDER_V01_ID =
            (MIN_ID_2 + 303);
    
    /**
    class com.splicemachine.db.iapi.services.io.FormatableLongHolder
     */
    int FORMATABLE_LONG_HOLDER_V01_ID =
            (MIN_ID_2 + 329);

    /**
    class com.splicemachine.db.iapi.services.io.FormatableHashtable
     */
    int FORMATABLE_HASHTABLE_V01_ID =
            (MIN_ID_2 + 313);
    
    /**
        class com.splicemachine.db.iapi.types.NationalCharTypeId
     */
    //static public final int NATIONAL_CHAR_TYPE_ID =
            //(MIN_ID_2 + 370);
    
    /**
        class com.splicemachine.db.iapi.types.NationalLongvarcharTypeId
     */
    //static public final int NATIONAL_LONGVARCHAR_TYPE_ID =
            //(MIN_ID_2 + 362);
    
    /**
        class com.splicemachine.db.iapi.types.NationalLongvarcharTypeId
     */
    //static public final int NCLOB_TYPE_ID = 
            //(MIN_ID_2 + 448);
    
    /**
        class com.splicemachine.db.iapi.types.NationalVarcharTypeId
     */
    //static public final int NATIONAL_VARCHAR_TYPE_ID =
            //(MIN_ID_2 + 369);

    /**
    class com.splicemachine.db.impl.sql.catalog.SchemaDescriptorFinder
     */
    int SCHEMA_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 371);
    
    /**
    class com.splicemachine.db.impl.sql.catalog.ColumnDescriptorFinder
     */
    int COLUMN_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 393);

    /**
    class com.splicemachine.db.impl.sql.catalog.SequenceDescriptorFinder
     */
    int SEQUENCE_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 472);

    int PERM_DESCRIPTOR_FINDER_V01_ID = (MIN_ID_2 + 473);
        
    /******************************************************************
    **
    **      DependencySystem Formats
    **
    **
    **
    ******************************************************************/
    /**
        Unused 243
     */
    int UNUSED_243 =
            (MIN_ID_2 + 243);
    
    /**
    ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    ||
    ||            DEPRECATED
    ||
    ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

    class com.splicemachine.db.impl.sql.catalog.OIDImpl
     */
    int OIDIMPL_V01_ID =
            (MIN_ID_2 + 15);
    
    /**
        class com.splicemachine.db.catalog.types.BooleanTypeIdImpl
     */
    int BOOLEAN_TYPE_ID_IMPL =
            (MIN_ID_2 + 16);

    /**
        class com.splicemachine.db.catalog.types.CharTypeIdImpl
     */
    int CHAR_TYPE_ID_IMPL =
            (MIN_ID_2 + 17);
    
    /**
        class com.splicemachine.db.catalog.types.DoubleTypeIdImpl
     */
    int DOUBLE_TYPE_ID_IMPL =
            (MIN_ID_2 + 18);
    
    /**
        class com.splicemachine.db.catalog.types.IntTypeIdImpl
     */
    int INT_TYPE_ID_IMPL =
            (MIN_ID_2 + 19);
    
    /**
        class com.splicemachine.db.catalog.types.RealTypeIdImpl
     */
    int REAL_TYPE_ID_IMPL =
            (MIN_ID_2 + 20);
    
    /**
        class com.splicemachine.db.catalog.types.RefTypeIdImpl
     */
    int REF_TYPE_ID_IMPL =
            (MIN_ID_2 + 21);
    
    /**
        class com.splicemachine.db.catalog.types.SmallintTypeIdImpl
     */
    int SMALLINT_TYPE_ID_IMPL =
            (MIN_ID_2 + 22);
    
    /**
        class com.splicemachine.db.catalog.types.LongintTypeIdImpl
     */
    int LONGINT_TYPE_ID_IMPL =
            (MIN_ID_2 + 23);
        
    /**
        class com.splicemachine.db.catalog.types.UserDefinedTypeIdImpl
     */
    //static public final int USERDEFINED_TYPE_ID_IMPL =
    //      (MIN_ID_2 + 24);

    /**
        class com.splicemachine.db.catalog.types.UserDefinedTypeIdImpl
     */
    //static public final int USERDEFINED_TYPE_ID_IMPL_V2 =
    //      (MIN_ID_2 + 264);

    /**
        class com.splicemachine.db.catalog.types.UserDefinedTypeIdImpl
     */
    int USERDEFINED_TYPE_ID_IMPL_V3 =
            (MIN_ID_2 + 264);
    /**
        class com.splicemachine.db.catalog.types.TypesImplInstanceGetter
     */
    int DATE_TYPE_ID_IMPL =
            (MIN_ID_2 + 32);

    /**
        class com.splicemachine.db.catalog.types.TypesImplInstanceGetter
     */
    int TIME_TYPE_ID_IMPL =
            (MIN_ID_2 + 33);
    /**
        class com.splicemachine.db.catalog.types.TypesImplInstanceGetter
     */
    int TIMESTAMP_TYPE_ID_IMPL =
            (MIN_ID_2 + 34);

    /**
        class com.splicemachine.db.Database.Language.Execution.MinAggregator
     */
    int AGG_MIN_V01_ID =
            (MIN_ID_2 + 153);

    /**
        class com.splicemachine.db.Database.Language.Execution.CountStarAggregator
     */
    int AGG_COUNT_STAR_V01_ID =
            (MIN_ID_2 + 150);


    /**
        class com.splicemachine.db.catalog.types.VarcharTypeIdImpl
     */
    int VARCHAR_TYPE_ID_IMPL =
            (MIN_ID_2 + 25);

    /**
        class com.splicemachine.db.impl.sql.catalog.ParameterDescriptorImpl
     */
    int PARAMETER_DESCRIPTOR_V01_ID =
            (MIN_ID_2 + 26);

    /**
        class com.splicemachine.db.iapi.types.BitTypeId
     */
    int BIT_TYPE_ID =
            (MIN_ID_2 + 27);

    /**
        class com.splicemachine.db.catalog.types.BitTypeIdImpl
     */
    int BIT_TYPE_ID_IMPL =
            (MIN_ID_2 + 28);

    /**
        class com.splicemachine.db.iapi.types.VarbitTypeId
     */
    int VARBIT_TYPE_ID =
            (MIN_ID_2 + 29);

    /**
        class com.splicemachine.db.iapi.types.VarbitTypeId
     */
    int VARBIT_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 258);
    
    /**
            class com.splicemachine.db.catalog.types.VarbitTypeIdImpl
     */
    int VARBIT_TYPE_ID_IMPL =
            (MIN_ID_2 + 30);


    /**
            class com.splicemachine.db.catalog.types.IndexDescriptorImpl
     */
    int INDEX_DESCRIPTOR_IMPL_V02_ID =
            (MIN_ID_2 + 387);
    

    /**
        class com.splicemachine.db.iapi.types.TinyintTypeId
     */
    int TINYINT_TYPE_ID =
            (MIN_ID_2 + 195);
    
    /**
            class com.splicemachine.db.catalog.types.TinyintTypeIdImpl
     */
    int TINYINT_TYPE_ID_IMPL =
            (MIN_ID_2 + 196);

    /**
        class com.splicemachine.db.iapi.types.DecimalTypeId
     */
    int DECIMAL_TYPE_ID =
            (MIN_ID_2 + 197);

    /**
        class com.splicemachine.db.iapi.types.DateTypeId
     */
    int DATE_TYPE_ID =
            (MIN_ID_2 + 40);

    /**
        class com.splicemachine.db.iapi.types.TimeTypeId
     */
    int TIME_TYPE_ID =
            (MIN_ID_2 + 35);

    /**
        class com.splicemachine.db.iapi.types.TimestampTypeId
     */
    int TIMESTAMP_TYPE_ID =
                (MIN_ID_2 + 36);
        
    /**
        class com.splicemachine.db.catalog.types.DecimalTypeIdImpl
     */
    int DECIMAL_TYPE_ID_IMPL =
            (MIN_ID_2 + 198);

    /**
        class com.splicemachine.db.catalog.types.LongvarcharTypeIdImpl
     */
    int LONGVARCHAR_TYPE_ID_IMPL =
            (MIN_ID_2 + 231);

    /**
        class com.splicemachine.db.catalog.types.LongvarcharTypeIdImpl
     */
    int CLOB_TYPE_ID_IMPL =
            (MIN_ID_2 + 446);

    /**
        class com.splicemachine.db.catalog.types.LongvarbitTypeIdImpl
            - does nto exist
     */
    int LONGVARBIT_TYPE_ID_IMPL =
            (MIN_ID_2 + 233);

    /**
        class com.splicemachine.db.catalog.types.LongvarbitTypeIdImpl
        - does not exist, 
        but we do it the same way for BLOB as for Longvarbit...
     */
    int BLOB_TYPE_ID_IMPL =
            (MIN_ID_2 + 442);

    /**
        class com.splicemachine.db.iapi.types.BitTypeId
     */
    int BIT_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 253);
    
    /**
        class com.splicemachine.db.iapi.types.DecimalTypeId
     */
    int DECIMAL_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 254);
    
    /**
        class com.splicemachine.db.iapi.types.TinyintTypeId
     */
    int TINYINT_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 257);

    /**
        class com.splicemachine.db.catalog.types.NationalCharTypeIdImpl
     */
    //static public final int NATIONAL_CHAR_TYPE_ID_IMPL =
            //(MIN_ID_2 + 366);

    /**
        class com.splicemachine.db.catalog.types.NationalVarcharTypeIdImpl
     */
    //static public final int NATIONAL_VARCHAR_TYPE_ID_IMPL =
            //(MIN_ID_2 + 367);

    /**
        class com.splicemachine.db.catalog.types.NationalLongVarcharTypeIdImpl
     */
    //static public final int NATIONAL_LONGVARCHAR_TYPE_ID_IMPL =
            //(MIN_ID_2 + 368);
    
    /**
        class com.splicemachine.db.catalog.types.NationalLongVarcharTypeIdImpl
     */
    //static public final int NCLOB_TYPE_ID_IMPL =
            //(MIN_ID_2 + 449);
    
    /**
        class com.splicemachine.db.iapi.types.XML (implementation of
        com.splicemachine.db.iapi.types.XMLDataValue).
     */
    int XML_TYPE_ID_IMPL =
            (MIN_ID_2 + 457);

    // 468 unused
    //        (MIN_ID_2 + 468);

    int ROW_MULTISET_TYPE_ID_IMPL =
            (MIN_ID_2 + 469);
    
    /******************************************************************
    **
    **      Execution MODULE CLASSES
    **
    ******************************************************************/

    /**
    class com.splicemachine.db.Database.Language.Execution.RenameConstantAction
    */
    int RENAME_CONSTANT_ACTION_V01_ID   =
            (MIN_ID_2 + 390);

    /**
        class com.splicemachine.db.Database.Language.Execution.DeleteConstantAction
     */
    int DELETE_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 37);

    /**
        class com.splicemachine.db.Database.Language.Execution.InsertConstantAction
     */
    int INSERT_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 38);

    /**
        class com.splicemachine.db.Database.Language.Execution.UpdateConstantAction
     */
    int UPDATABLE_VTI_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 375);

    /**
        class com.splicemachine.db.Database.Language.Execution.UpdateConstantAction
     */
    int UPDATE_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 39);

    /**
     */
    int UNUSED_2_204 =
            (MIN_ID_2 + 204);

    /**
        UNUSED
     */
    int UNUSED_2_41 =
            (MIN_ID_2 + 41);
    
    /**
    class com.splicemachine.db.Database.Language.Execution.DropAliasConstantAction
    */
    int UNUSED_2_42 =
            (MIN_ID_2 + 42);
    
    /**
    class com.splicemachine.db.Database.Language.Execution.CreateSchemaConstantAction
    */
    int UNUSED_2_141    =
            (MIN_ID_2 + 141);
    
    /**
    */
    int UNUSED_2_142    =
            (MIN_ID_2 + 142);
    
    /**
    class com.splicemachine.db.Database.Language.Execution.CreateViewConstantAction
    */
    int UNUSED_2_143    =
            (MIN_ID_2 + 143);
    
    /**
    */
    int UNUSED_2_144    =
            (MIN_ID_2 + 144);

    /**
        class com.splicemachine.db.Database.Language.Execution.ProviderInfo
     */
    int PROVIDER_INFO_V01_ID =
            (MIN_ID_2 + 148);

    /**
        class com.splicemachine.db.Database.Language.Execution.AvgAggregator
     */
    int AGG_AVG_V01_ID =
            (MIN_ID_2 + 149);

    /**
        class com.splicemachine.db.Database.Language.Execution.CountAggregator
     */
    int AGG_COUNT_V01_ID =
            (MIN_ID_2 + 151);

    /**
        class com.splicemachine.db.Database.Language.Execution.MaxMinAggregator
     */
    int AGG_MAX_MIN_V01_ID =
            (MIN_ID_2 + 152);

    /**
        class com.splicemachine.db.Database.Language.Execution.SumAggregator
     */
    int AGG_SUM_V01_ID =
            (MIN_ID_2 + 154);

    /**
     class com.splicemachine.db.Database.Language.Execution.UserAggregatorAggregator
    */
    int AGG_USER_ADAPTOR_V01_ID =
            (MIN_ID_2 + 323);

    /**
        class com.splicemachine.db.Database.Language.Execution.StatisticsConstantAction
    */
    int STATISTICS_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 155);

    /**
        class com.splicemachine.db.Database.Language.Execution.LockTableConstantAction
    */
    int UNUSED_2_275 =
            (MIN_ID_2 + 275);


    /**
    class com.splicemachine.db.Database.Language.Execution.CreateSPSConstantAction
    */
    int UNUSED_2_221    =
            (MIN_ID_2 + 221);
    
    /**
    class com.splicemachine.db.Database.Language.Execution.CreateSPSConstantAction
    */
    int UNUSED_2_222    =
            (MIN_ID_2 + 222);

    /**
    class com.splicemachine.db.Database.Language.Execution.AlterSPSConstantAction
    */
    int ALTER_SPS_CONSTANT_ACTION_V01_ID        =
            (MIN_ID_2 + 229);

    /**
    class com.splicemachine.db.Database.Language.Execution.IndexColumnOrder
    */
    int INDEX_COLUMN_ORDER_V01_ID       =
            (MIN_ID_2 + 218);

    /**
    class com.splicemachine.db.Database.Language.Execution.AggregateInfo
    */
    int AGG_INFO_V01_ID =
            (MIN_ID_2 + 223);

    /**
     class com.splicemachine.db.Database.Language.Execution.AggregateInfo
     */
    int AGG_INFO_V02_ID =
            (MIN_ID_2 + 483);

    /**
    class com.splicemachine.db.Database.Language.Execution.AggregateInfoList
    */
    int AGG_INFO_LIST_V01_ID    =
            (MIN_ID_2 + 224);

    /**
       class com.splicemachine.db.Database.Language.Execution.DeleteConstantAction
       This class is abstract so it doesn't need a format id!
     */
    int WRITE_CURSOR_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 227);

    /**
     * 237 - unused
      */
    //static public final int VALUE_ROW_V01_ID =
    //        (MIN_ID_2 + 237);

    /**
      238 unused
     */
    //static public final int INDEX_ROW_V01_ID =
    //       (MIN_ID_2 + 238);

    /**
      class com.splicemachine.db.impl.sql.execute.AddJarConstantAction;
     */
    int ADD_JAR_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 211);

    /**
      class com.splicemachine.db.impl.sql.execute.DropJarConstantAction;
     */
    int DROP_JAR_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 212);

    /**
      class com.splicemachine.db.impl.sql.execute.ReplaceJarConstantAction;
     */
    int REPLACE_JAR_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 213);

     /**
    class com.splicemachine.db.Database.Language.Execution.ConstraintInfo
     */
     int CONSTRAINT_INFO_V01_ID  =
            (MIN_ID_2 + 278);

    /**
     */
    int UNUSED_2_280 =
            (MIN_ID_2 + 280);

    /**
    class com.splicemachine.db.Database.Language.Execution.FKInfo
     */
    int FK_INFO_V01_ID  =
            (MIN_ID_2 + 282);

    /**
     */
    int UNUSED_2_289    =
            (MIN_ID_2 + 289);
    
    /**
    class com.splicemachine.db.impl.sql.execute.CreateTriggerConstantAction
     */
    int CREATE_TRIGGER_CONSTANT_ACTION_V01_ID   =
            (MIN_ID_2 + 314);
    
    /**
    class com.splicemachine.db.impl.sql.execute.DropTriggerConstantAction
     */
    int DROP_TRIGGER_CONSTANT_ACTION_V01_ID     =
            (MIN_ID_2 + 315);

    /**
    class com.splicemachine.db.impl.sql.execute.TriggerInfo
     */
    int TRIGGER_INFO_V01_ID     =
            (MIN_ID_2 + 317);

    /**
    class com.splicemachine.db.impl.sql.execute.TransactionConstantAction
     */
    int TRANSACTION_CONSTANT_ACTION_V01_ID      =
            (MIN_ID_2 + 318);

    /**
    class com.splicemachine.db.Database.Language.Execution.SetTriggersConstantAction
     */
    int SET_TRIGGERS_CONSTANT_ACTION_V01_ID     =
            (MIN_ID_2 + 321);

    /**
        class com.splicemachine.db.Replication.Database.Language.Execution.RepSetTriggersConstantAction
     */
    int REP_SET_TRIGGERS_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 322);

    /**
        class com.splicemachine.db.impl.sql.execute.RealLastIndexKeyScanStatistics
     */
    int REAL_LAST_INDEX_KEY_SCAN_STATISTICS_IMPL_V01_ID =
            (MIN_ID_2 + 327);

    ////////////////////////////////////////////////////////////////////////////
    //
    // New versions of 2.0 Language ConstantActions, versioned in 3.0
    //
    ////////////////////////////////////////////////////////////////////////////


    /** class com.splicemachine.db.Database.Language.Execution.SetSchemaConstantAction */
    int SET_SCHEMA_CONSTANT_ACTION_V02_ID                       = (MIN_ID_2 + 353);

    /** class com.splicemachine.db.Database.Language.Execution.SetTransactionIsolationConstantAction */
    int SET_TRANSACTION_ISOLATION_CONSTANT_ACTION_V02_ID = (MIN_ID_2 + 354);

    /** class com.splicemachine.db.impl.sql.execute.ColumnInfo */
    int COLUMN_INFO_V02_ID                      = (MIN_ID_2 + 358);

    /** class com.splicemachine.db.Database.Language.DependencySystem.Generic.ProviderInfo */
    int PROVIDER_INFO_V02_ID                    = (MIN_ID_2 + 359);

    /** class com.splicemachine.db.impl.sql.execute.SavepointConstantAction */
    int SAVEPOINT_V01_ID                      = (MIN_ID_2 + 452);

    /******************************************************************
    **
    **      LanguageInterface MODULE CLASSES
    **
    ******************************************************************/
    /**
    class com.splicemachine.db.impl.sql.GenericStorablePreparedStatement
     */
    int STORABLE_PREPARED_STATEMENT_V01_ID      =
            (MIN_ID_2 + 225);
    
    /**
    class com.splicemachine.db.impl.sql.GenericResultDescription
     */
    int GENERIC_RESULT_DESCRIPTION_V01_ID       =
            (MIN_ID_2 + 228);

    /**
    UNUSED
     */
    int UNUSED_2_215    = (MIN_ID_2 + 215);

    /**
    class com.splicemachine.db.impl.sql.GenericTypeDescriptor
     */
    int GENERIC_TYPE_DESCRIPTOR_V01_ID  =
            (MIN_ID_2 + 216);

    /**
    class com.splicemachine.db.impl.sql.GenericTypeId
     */
    int GENERIC_TYPE_ID_V01_ID  =
            (MIN_ID_2 + 217);

    /**
    class com.splicemachine.db.impl.sql.CursorTableReference
     */
    int CURSOR_TABLE_REFERENCE_V01_ID   =
            (MIN_ID_2 + 296);

    /**
    class com.splicemachine.db.impl.sql.CursorInfo
     */
    int CURSOR_INFO_V01_ID      =
            (MIN_ID_2 + 297);

    /******************************************************************
    **
    **      ALIAS INFO CLASSES
    **
    ******************************************************************/

    /**
    class com.splicemachine.db.catalog.types.ClassAliasInfo
     */
    int CLASS_ALIAS_INFO_V01_ID =
            (MIN_ID_2 + 310);

    /**
    class com.splicemachine.db.catalog.types.MethodAliasInfo
     */
    int METHOD_ALIAS_INFO_V01_ID        =
            (MIN_ID_2 + 312);

    /**
    class com.splicemachine.db.catalog.types.WorkUnitAliasInfo
     */
    int WORK_UNIT_ALIAS_INFO_V01_ID     =
            (MIN_ID_2 + 309);

    /**
    class com.splicemachine.db.catalog.types.UserAggregateAliasInfo
     */
    int USER_AGGREGATE_ALIAS_INFO_V01_ID        =
            (MIN_ID_2 + 311);



    int ROUTINE_INFO_V01_ID = (MIN_ID_2 + 451);
    int SYNONYM_INFO_V01_ID = (MIN_ID_2 + 455);
    int UDT_INFO_V01_ID = (MIN_ID_2 + 474);
    int AGGREGATE_INFO_V01_ID = (MIN_ID_2 + 475);
    /**
     class com.splicemachine.db.Database.Language.Execution.WindowFunctionInfo
     */
    int WINDOW_FUNCTION_INFO_V01_ID = (MIN_ID_2 + 476);
    /**
     class com.splicemachine.db.Database.Language.Execution.WindowFunctionInfoList
     */
    int WINDOW_FUNCTION_INFO_LIST_V01_ID = (MIN_ID_2 + 477);

    /******************************************************************
    **
    **	DEFAULT INFO CLASSES
    **
    ******************************************************************/
            
    /**
    class com.splicemachine.db.catalog.types.DefaultInfoImpl
     */
    int DEFAULT_INFO_IMPL_V01_ID =
            (MIN_ID_2 + 326);





    /**
    class com.splicemachine.db.impl.sql.GenericColumnDescriptor
     */
    int GENERIC_COLUMN_DESCRIPTOR_V02_ID        =
            (MIN_ID_2 + 383);


    /**
            UNUSED (MIN_ID_2 + 384)
    */

    /**
        UNUSED (MIN_ID_2 + 382)
     */

    


    /******************************************************************
    **
    **  Type system id's
    **
    ******************************************************************/

    int SQL_BOOLEAN_ID =
            (MIN_ID_2 + 77);

    int SQL_CHAR_ID =
            (MIN_ID_2 + 78);

    int SQL_DOUBLE_ID =
            (MIN_ID_2 + 79);

    int SQL_INTEGER_ID =
            (MIN_ID_2 + 80);

    int SQL_REAL_ID =
            (MIN_ID_2 + 81);

    int SQL_REF_ID =
            (MIN_ID_2 + 82);

    int SQL_SMALLINT_ID =
            (MIN_ID_2 + 83);

    int SQL_LONGINT_ID =
            (MIN_ID_2 + 84);

    int SQL_VARCHAR_ID =
            (MIN_ID_2 + 85);

    int LIST_ID =
        (MIN_ID_2 + 478);
    
    int LIST_TYPE_ID_IMPL =
        (MIN_ID_2 + 480);
    
    int LIST_TYPE_ID =
        (MIN_ID_2 + 481);

    /**
     class com.splicemachine.db.Database.Language.Execution.StringAggregator
     */
    int AGG_STR_V01_ID =
            (MIN_ID_2 + 482);


    //public static final int SQL_USERTYPE_ID = 
    //      (MIN_ID_2 + 86);

    //public static final int SQL_USERTYPE_ID_V2 = 
    //      (MIN_ID_2 + 266);

    int SQL_USERTYPE_ID_V3 =
            (MIN_ID_2 + 266);

    int SQL_DATE_ID =
            (MIN_ID_2 + 298);

    int SQL_TIME_ID =
            (MIN_ID_2 + 299);

    int SQL_TIMESTAMP_ID =
            (MIN_ID_2 + 31);

    int SQL_BIT_ID =
            (MIN_ID_2 + 87);

    int SQL_VARBIT_ID =
            (MIN_ID_2 + 88);

    int SQL_TINYINT_ID =
            (MIN_ID_2 + 199);

    int SQL_DECIMAL_ID =
            (MIN_ID_2 + 200);

    int SQL_LONGVARCHAR_ID =
            (MIN_ID_2 + 235);

    int SQL_CLOB_ID =
            (MIN_ID_2 + 447);

    int SQL_LONGVARBIT_ID =
            (MIN_ID_2 + 234);

    int SQL_BLOB_ID =
            (MIN_ID_2 + 443);

    //public static final int SQL_NATIONAL_CHAR_ID = 
            //(MIN_ID_2 + 363);

    //public static final int SQL_NATIONAL_VARCHAR_ID = 
            //(MIN_ID_2 + 364);

    //public static final int SQL_NATIONAL_LONGVARCHAR_ID = 
            //(MIN_ID_2 + 365);

    //public static final int SQL_NCLOB_ID = 
            //(MIN_ID_2 + 450);

    // Interface: com.splicemachine.db.iapi.types.XMLDataValue
    int XML_ID =
            (MIN_ID_2 + 458);

    /******************************************************************
    ** 
    ** Access ids.
    **
    **
    **
    ******************************************************************/
    int ACCESS_U8_V1_ID =
            (MIN_ID_2 + 89);

    int ACCESS_HEAP_ROW_LOCATION_V1_ID =
            (MIN_ID_2 + 90);

    int ACCESS_HEAP_V2_ID =
            (MIN_ID_2 + 91);

    int ACCESS_B2I_V2_ID =
            (MIN_ID_2 + 92);

    int ACCESS_FORMAT_ID =
            (MIN_ID_2 + 93);

    int ACCESS_T_STRINGCOLUMN_ID =
            (MIN_ID_2 + 94);

    int ACCESS_B2IUNDO_V1_ID =
            (MIN_ID_2 + 95);

    // Deleted as part of 7.2 rebrand project.

    /*
    public static final int ACCESS_CONGLOMDIR_V1_ID =
            (MIN_ID_2 + 96);
    */

    int ACCESS_BTREE_LEAFCONTROLROW_V1_ID =
            (MIN_ID_2 + 133);

    int ACCESS_BTREE_BRANCHCONTROLROW_V1_ID =
            (MIN_ID_2 + 134);

    int ACCESS_SERIALIZABLEWRAPPER_V1_ID =
            (MIN_ID_2 + 202);

    int ACCESS_B2I_STATIC_COMPILED_V1_ID =
            (MIN_ID_2 + 360);

    int ACCESS_TREE_V1_ID =
            (MIN_ID_2 + 386);


    int ACCESS_B2I_V3_ID =
            (MIN_ID_2 + 388);

    int ACCESS_GISTUNDO_V1_ID =
            (MIN_ID_2 + 389);

    int ACCESS_GIST_LEAFCONTROLROW_V1_ID =
            (MIN_ID_2 + 394);

    int ACCESS_GIST_BRANCHCONTROLROW_V1_ID =
            (MIN_ID_2 + 395);

    int STATISTICS_IMPL_V01_ID =
            (MIN_ID_2 + 397);

    int UPDATE_STATISTICS_CONSTANT_ACTION_ID =
            (MIN_ID_2 +     398);

    int DROP_STATISTICS_CONSTANT_ACTION_ID =
            (MIN_ID_2 + 399);

    int ACCESS_GIST_RTREE_V1_ID =
            (MIN_ID_2 + 400);

    int ACCESS_T_RECTANGLE_ID =
            (MIN_ID_4 + 34);

    int ACCESS_T_INTCOL_V1_ID =
            (MIN_ID_4 + 4);

    int ACCESS_B2I_V4_ID =
            (MIN_ID_2 + 466);

    int ACCESS_HEAP_V3_ID =
            (MIN_ID_2 + 467);

    int ACCESS_B2I_V5_ID =
            (MIN_ID_2 + 470);

    public static final int ACCESS_B2I_V6_ID =
            (MIN_ID_2 + 479);
    /******************************************************************
    **
    ** PropertyConglomerate
    ** 
    ** 
    ** 
    ******************************************************************/
    /** class com.splicemachine.db.impl.store.access.PropertyConglomerate */

    int PC_XENA_VERSION_ID =
            (MIN_ID_2 + 15);


    /******************************************************************
    **
    ** Raw Store Log operation Ids
    **
    **
    **
    ******************************************************************/

    /* com.splicemachine.db.impl.store.raw.data.ChainAllocPageOperation */
    int LOGOP_CHAIN_ALLOC_PAGE =
            (MIN_ID_2 + 97);

    /* com.splicemachine.db.impl.store.raw.xact.BeginXact */
    int LOGOP_BEGIN_XACT =
            (MIN_ID_2 + 169);

    /* com.splicemachine.db.impl.store.raw.log.CheckpointOperation */
    int LOGOP_CHECKPOINT =
            (MIN_ID_2 + 263);

    /* com.splicemachine.db.impl.store.raw.data.ContainerOperation */
    /* creating, dropping, removing container */
    int LOGOP_CONTAINER =
            (MIN_ID_2 + 242);

    /* com.splicemachine.db.impl.store.raw.data.DeleteOperation */
    int LOGOP_DELETE =
            (MIN_ID_2 + 101);

    /* com.splicemachine.db.impl.store.raw.xact.EndXact */
    int LOGOP_END_XACT =
            (MIN_ID_2 + 102);

    /* com.splicemachine.db.impl.store.raw.data.InsertOperation */
    int LOGOP_INSERT =
            (MIN_ID_2 + 103);

    /* com.splicemachine.db.impl.store.raw.data.LogicalUndoOperation */
    int LOGOP_PAGE_LOGICAL_UNDO =
            (MIN_ID_2 + 104);

    /* com.splicemachine.db.impl.store.raw.data.PhysicalUndoOperation */
    int LOGOP_PAGE_PHYSICAL_UNDO =
            (MIN_ID_2 + 105);

    /* com.splicemachine.db.impl.store.raw.data.PurgeOperation */
    int LOGOP_PURGE =
            (MIN_ID_2 + 106);

    /* com.splicemachine.db.impl.store.raw.data.ContainerUndoOperation */
    int LOGOP_CONTAINER_UNDO =
            (MIN_ID_2 + 107);

    /* com.splicemachine.db.impl.store.raw.data.UpdateOperation */
    int LOGOP_UPDATE =
            (MIN_ID_2 + 108);

    /* com.splicemachine.db.impl.store.raw.data.UpdateFieldOperation */
    int LOGOP_UPDATE_FIELD =
            (MIN_ID_2 + 109);

    /* com.splicemachine.db.impl.store.raw.data.CopyRowsOperation */
    int LOGOP_COPY_ROWS =
            (MIN_ID_2 + 210);

    /* com.splicemachine.db.impl.store.raw.data.AllocPageOperation */
    int LOGOP_ALLOC_PAGE =
            (MIN_ID_2 + 111);

    /*com.splicemachine.db.impl.store.raw.data.InitPageOperation */
    int LOGOP_INIT_PAGE =
            (MIN_ID_2 + 241);

    /* com.splicemachine.db.impl.store.raw.data.InvalidatePageOperation */
    int LOGOP_INVALIDATE_PAGE =
            (MIN_ID_2 + 113);

    /* com.splicemachine.db.impl.store.raw.data.SetReservedSpaceOperation */
    int LOGOP_SET_RESERVED_SPACE =
            (MIN_ID_2 + 287);

    /* com.splicemachine.db.impl.store.raw.data.RemoveFileOperation */
    int LOGOP_REMOVE_FILE =
            (MIN_ID_2 + 291);

    /* com.splicemachine.db.impl.store.raw.log.ChecksumOperation */
    int LOGOP_CHECKSUM =
            (MIN_ID_2 + 453);

    /* com.splicemachine.db.impl.store.raw.data.CompressSpacePageOperation10_2 */
    int LOGOP_COMPRESS10_2_SPACE =
            (MIN_ID_2 + 454);

    /* com.splicemachine.db.impl.store.raw.data.CompressSpacePageOperation */
    int LOGOP_COMPRESS_SPACE =
            (MIN_ID_2 + 465);

    /* com.splicemachine.db.impl.store.raw.data.EncryptContainerOperation */
    int LOGOP_ENCRYPT_CONTAINER =
            (MIN_ID_2 + 459);

    /* com.splicemachine.db.impl.store.raw.data.EncryptContainerUndoOperation */
    int LOGOP_ENCRYPT_CONTAINER_UNDO =
            (MIN_ID_2 + 460);

    /*******************************************************************
    **
    ** container types
    **
    ******************************************************************/
    /* com.splicemachine.db.impl.store.raw.data.FileContainer */
    int RAW_STORE_SINGLE_CONTAINER_FILE =
            (MIN_ID_2 + 116);

    /* com.splicemachine.db.impl.store.raw.data.StreamFileContainer */
    int RAW_STORE_SINGLE_CONTAINER_STREAM_FILE =
            (MIN_ID_2 + 290);

    /*******************************************************************
    **
    ** page types
    **
    **
    ******************************************************************/
    /* com.splicemachine.db.impl.store.raw.data.StoredPage */
    int RAW_STORE_STORED_PAGE =
            (MIN_ID_2 + 117);

    /* com.splicemachine.db.impl.store.raw.data.AllocPage */
    int RAW_STORE_ALLOC_PAGE =
            (MIN_ID_2 + 118);


    /*****************************************************************
    **
    ** Log files
    **
    **
    ******************************************************************/
    /* com.splicemachine.db.impl.store.raw.log.LogToFile */
    int FILE_STREAM_LOG_FILE =
            (MIN_ID_2 + 128);


    /*****************************************************************
    **
    ** Log record
    **
    ******************************************************************/
    /* com.splicemachine.db.impl.store.raw.log.LogRecord */
    int LOG_RECORD =
            (MIN_ID_2 + 129);

    /* com.splicemachine.db.impl.store.raw.log.LogCounter */
    int LOG_COUNTER =
            (MIN_ID_2 + 130);

    /******************************************************************
    **
    **  identifiers
    **
    ******************************************************************/
    /* com.splicemachine.db.impl.services.uuid.BasicUUID */
    int BASIC_UUID =
            (MIN_ID_2 + 131);

    /*
     *      Transaction Ids
     */

    /* com.splicemachine.db.impl.store.raw.xact.GlobalXactId */
    int RAW_STORE_GLOBAL_XACT_ID_V20 =
            (MIN_ID_2 + 132);

    /* com.splicemachine.db.impl.store.raw.xact.XactId */
    int RAW_STORE_XACT_ID =
            (MIN_ID_2 + 147);

    /* com.splicemachine.db.impl.store.raw.xact.XAXactId */
    int RAW_STORE_GLOBAL_XACT_ID_NEW =
            (MIN_ID_2 + 328);

    /*
     * Transaction table
     */
    /* com.splicemachine.db.impl.store.raw.xact.TransactionTableEntry */
    int RAW_STORE_TRANSACTION_TABLE_ENTRY =
            (MIN_ID_2 + 261);

    /* com.splicemachine.db.impl.store.raw.xact.TransactionTable */
    int RAW_STORE_TRANSACTION_TABLE =
            (MIN_ID_2 + 262);

            
    /******************************************************************
    **
    **  LocalDriver Formatables.
    **
    ******************************************************************/

    /* NOT USED = com.splicemachine.db.impl.jdbc.ExternalizableConnection */
    int EXTERNALIZABLE_CONNECTION_ID = (MIN_ID_2 + 192);


    /******************************************************************
    **
    **      InternalUtils MODULE CLASSES
    **
    ******************************************************************/
    /* com.splicemachine.db.iapi.util.ByteArray */
    int FORMATABLE_BYTE_ARRAY_V01_ID = (MIN_ID_2 + 219);


   /******************************************************************
    **
    **  UDPATE MAX_ID_2 WHEN YOU ADD A NEW FORMATABLE
    **
    ******************************************************************/


    /*
     * Make sure this is updated when a new module is added
     */
   int MAX_ID_2 =
    		(MIN_ID_2 + 483);

    // DO NOT USE 4 BYTE IDS ANYMORE
    int MAX_ID_4 =
            (MIN_ID_4 + 34);

    int SQL_ARRAY_ID =
            (MIN_ID_2 + 508);

    int ARRAY_TYPE_ID =
            (MIN_ID_2 + 700);

    int ARRAY_TYPE_ID_IMPL =
            (MIN_ID_2 + 704);


}
