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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
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
    static public final int NULL_FORMAT_ID =
            (MIN_ID_2 + 0);

    /** Special format id for tagging UTF8 strings */
    static public final int STRING_FORMAT_ID =
            (MIN_ID_2 + 1);

    /** Special format id for tagging Serializable objects. */
    static public final int SERIALIZABLE_FORMAT_ID =
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
    static public final int BOOLEAN_TYPE_ID =
            (MIN_ID_2 + 4);
    
    /**
        class com.splicemachine.db.iapi.types.BooleanTypeId
     */
    static public final int BOOLEAN_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 260);
    
    /**
        class com.splicemachine.db.iapi.types.CharTypeId
     */
    static public final int CHAR_TYPE_ID =
            (MIN_ID_2 + 5);

    /**
        class com.splicemachine.db.iapi.types.CharTypeId
     */
    static public final int CHAR_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 244);

    /**
        class com.splicemachine.db.iapi.types.DoubleTypeId
     */
    static public final int DOUBLE_TYPE_ID =
            (MIN_ID_2 + 6);
    
    /**
        class com.splicemachine.db.iapi.types.DoubleTypeId
     */
    static public final int DOUBLE_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 245);
    
    /**
        class com.splicemachine.db.iapi.types.IntTypeId
     */
    static public final int INT_TYPE_ID =                            
            (MIN_ID_2 + 7);

    /**
        class com.splicemachine.db.iapi.types.IntTypeId
     */
    static public final int INT_COMPILATION_TYPE_ID =                                
            (MIN_ID_2 + 246);

    /**
        class com.splicemachine.db.iapi.types.RealTypeId
     */
    static public final int REAL_TYPE_ID =
            (MIN_ID_2 + 8);

    /**
        class com.splicemachine.db.iapi.types.RealTypeId
     */
    static public final int REAL_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 247);

    /**
        class com.splicemachine.db.iapi.types.RefTypeId
     */
    static public final int REF_TYPE_ID =
            (MIN_ID_2 + 9);

    /**
        class com.splicemachine.db.iapi.types.RefTypeId
     */
    static public final int REF_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 248);
    
    /**
        class com.splicemachine.db.iapi.types.SmallintTypeId
     */
    static public final int SMALLINT_TYPE_ID =
            (MIN_ID_2 + 10);
    
    /**
        class com.splicemachine.db.iapi.types.SmallintTypeId
     */
    static public final int SMALLINT_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 249);
    
    /**
        class com.splicemachine.db.iapi.types.LongintTypeId
     */
    static public final int LONGINT_TYPE_ID =
            (MIN_ID_2 + 11);
    
    /**
        class com.splicemachine.db.iapi.types.LongintTypeId
     */
    static public final int LONGINT_COMPILATION_TYPE_ID =
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
    static public final int USERDEFINED_TYPE_ID_V3 =
            (MIN_ID_2 + 267);
    
    /**
        class com.splicemachine.db.iapi.types.UserDefinedTypeId
     */
    static public final int USERDEFINED_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 251);
    
    /**
        class com.splicemachine.db.iapi.types.UserDefinedTypeIdV2
     */
    static public final int USERDEFINED_COMPILATION_TYPE_ID_V2 =
            (MIN_ID_2 + 265);
    
    /**
        class com.splicemachine.db.iapi.types.VarcharTypeId
     */
    static public final int VARCHAR_TYPE_ID =
            (MIN_ID_2 + 13);
    
    /**
        class com.splicemachine.db.iapi.types.VarcharTypeId
     */
    static public final int VARCHAR_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 252);
    
    /**
    class com.splicemachine.db.catalog.types.TypeDescriptorImpl
    */
    static public final int DATA_TYPE_IMPL_DESCRIPTOR_V01_ID =
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
    static public final int DATA_TYPE_SERVICES_IMPL_V01_ID =
            (MIN_ID_2 + 259);

    /**
    class com.splicemachine.db.impl.sql.catalog.ConglomerateDescriptorFinder
     */
    static public final int CONGLOMERATE_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 135);
    
    /**
    class com.splicemachine.db.impl.sql.catalog.ConstraintDescriptorFinder
     */
    static public final int CONSTRAINT_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 208);
    
    /**
    class com.splicemachine.db.impl.sql.catalog.DefaultDescriptorFinder
     */
    static public final int DEFAULT_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 325);
    
    /**
    class com.splicemachine.db.impl.sql.catalog.AliasDescriptorFinder
     */
    static public final int ALIAS_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 136);

    /**
    class com.splicemachine.db.impl.sql.catalog.TableDescriptorFinder
     */
    static public final int TABLE_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 137);

    /**
    class com.splicemachine.db.impl.sql.catalog.CoreDDFinderClassInfo
     */
    static public final int ROUTINE_PERMISSION_FINDER_V01_ID =
            (MIN_ID_2 + 461);

    /**
    class com.splicemachine.db.impl.sql.catalog.CoreDDFinderClassInfo
     */
    static public final int TABLE_PERMISSION_FINDER_V01_ID =
            (MIN_ID_2 + 462);

    /**
    class com.splicemachine.db.impl.sql.catalog.CoreDDFinderClassInfo
     */
    static public final int COLUMNS_PERMISSION_FINDER_V01_ID =
            (MIN_ID_2 + 463);

    /**
     class com.splicemachine.db.impl.sql.catalog.CoreDDFinderClassInfo
     */
    static public final int SCHEMA_PERMISSION_FINDER_V01_ID =
            (MIN_ID_2 + 464);

    /**
    class com.splicemachine.db.impl.sql.catalog.CoreDDFinderClassInfo
     */
    static public final int ROLE_GRANT_FINDER_V01_ID =
            (MIN_ID_2 + 471);
    
    /**
    class com.splicemachine.db.impl.sql.catalog.DataDictionaryDescriptorFinder
     */
    static public final int DATA_DICTIONARY_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 138);
    
    /**
    class com.splicemachine.db.impl.sql.catalog.ViewDescriptorFinder
     */
    static public final int VIEW_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 145);
    
    /**
    class com.splicemachine.db.impl.sql.catalog.SPSDescriptorFinder
     */
    static public final int SPS_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 226);

    /**
    class com.splicemachine.db.impl.sql.catalog.FileInfoFinder
     */
    static public final int FILE_INFO_FINDER_V01_ID =
            (MIN_ID_2 + 273);

    /**
    class com.splicemachine.db.impl.sql.catalog.TriggerDescriptorFinder
     */
    static public final int TRIGGER_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 320);

    /**
    class com.splicemachine.db.impl.sql.catalog.TriggerDescriptorFinder
     */
    static public final int TRIGGER_DESCRIPTOR_V01_ID =
            (MIN_ID_2 + 316);

    /**
    class com.splicemachine.db.impl.sql.catalog.DD_SocratesVersion
     */
    static public final int DD_SOCRATES_VERSION_ID =
            (MIN_ID_2 + 174);
    
    /**
    class com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl
     */
    static public final int REFERENCED_COLUMNS_DESCRIPTOR_IMPL_V01_ID =
            (MIN_ID_2 + 205);
    
    /**
    class com.splicemachine.db.impl.sql.catalog.DD_PlatoVersion
     */
    static public final int DD_PLATO_VERSION_ID =
            (MIN_ID_2 + 206);

    /**
    class com.splicemachine.db.impl.sql.catalog.DD_AristotleVersion
     */
    static public final int DD_ARISTOTLE_VERSION_ID =
            (MIN_ID_2 + 272);

    /**
      class com.splicemachine.db.impl.sql.catalog.DD_XenaVersion
     */
    static public final int DD_XENA_VERSION_ID =
            (MIN_ID_2 + 302);

    /**
      class com.splicemachine.db.impl.sql.catalog.DD_BuffyVersion
     */
    static public final int DD_BUFFY_VERSION_ID =
            (MIN_ID_2 + 373);

    /**
      class com.splicemachine.db.impl.sql.catalog.DD_MulanVersion
     */
    static public final int DD_MULAN_VERSION_ID =
            (MIN_ID_2 + 376);

    /**
      class com.splicemachine.db.impl.sql.catalog.DD_IvanovaVersion
     */
    static public final int DD_IVANOVA_VERSION_ID =
            (MIN_ID_2 + 396);

    /**
      class com.splicemachine.db.impl.sql.catalog.DD_DB2J72
      now mapped to a single class DD_Version.
      5.0 databases will have this as the format identifier for their
      catalog version number.
     */
    static public final int DD_DB2J72_VERSION_ID =
            (MIN_ID_2 + 401);

    /**
      class com.splicemachine.db.impl.sql.catalog.DD_Version
      now mapped to a single class DD_Version.
      5.1 and later databases will have this as the format identifier for their
      catalog version number.
    */
    static public final int DD_ARWEN_VERSION_ID =
            (MIN_ID_2 + 402);
    
    /**
            class com.splicemachine.db.iapi.types.LongvarcharTypeId
     */
    static public final int LONGVARCHAR_TYPE_ID =
            (MIN_ID_2 + 230);
    
    /**
            class com.splicemachine.db.iapi.types.LongvarcharTypeId
     */
    static public final int LONGVARCHAR_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 256);

    /**
            class com.splicemachine.db.iapi.types.LongvarcharTypeId
     */
    static public final int CLOB_TYPE_ID =
            (MIN_ID_2 + 444);
    
    /**
            class com.splicemachine.db.iapi.types.LongvarcharTypeId
     */
    static public final int CLOB_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 445);

    /**
            class com.splicemachine.db.iapi.types.LongvarbitTypeId
            - XXXX does not exist!!!
     */
    static public final int LONGVARBIT_TYPE_ID =
            (MIN_ID_2 + 232);

    /**
            class com.splicemachine.db.iapi.types.LongvarbitTypeId
            - XXXX does not exist!!!
     */
    static public final int LONGVARBIT_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 255);

    /**
            class com.splicemachine.db.iapi.types.LongvarbitTypeId
            - XXXX does not exist!!!
    But for BLOB we do the same as for LONGVARBIT, only need different ids
     */
    static public final int BLOB_TYPE_ID =
            (MIN_ID_2 + 440);

    /**
            class com.splicemachine.db.iapi.types.LongvarbitTypeId
            - XXXX does not exist!!!
    But for BLOB we do the same as for LONGVARBIT, only need different ids
     */
    static public final int BLOB_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 441);

    /**
            Instance of TypeId for XML data types.
     */
    static public final int XML_TYPE_ID =
            (MIN_ID_2 + 456);
    
    /**
        class com.splicemachine.db.iapi.types.JSQLType
     */
    static public final int JSQLTYPEIMPL_ID =
            (MIN_ID_2 + 307);

    /**
    class com.splicemachine.db.impl.sql.catalog.RowListImpl
     */
    static public final int ROW_LIST_V01_ID =
            (MIN_ID_2 + 239);
    
    /**
     * DataTypeDescriptor (runtime type) new format from 10.4
     * onwards that reflects the change in role from is a TypeDescriptor
     * to has a TypeDescriptor. Fixes the format so that information
     * is not duplicated.
     * Old format number was DATA_TYPE_SERVICES_IMPL_V01_ID (259).
     */
    static public final int DATA_TYPE_DESCRIPTOR_V02_ID =
            (MIN_ID_2 + 240);

    /**
    class com.splicemachine.db.impl.sql.catalog.IndexRowGeneratorImpl
     */
    static public final int INDEX_ROW_GENERATOR_V01_ID =
            (MIN_ID_2 + 268);

    /**
    class com.splicemachine.db.iapi.services.io.FormatableBitSet
     */
    static public final int BITIMPL_V01_ID =
            (MIN_ID_2 + 269);

    /**
    class com.splicemachine.db.iapi.services.io.FormatableArrayHolder
     */
    static public final int FORMATABLE_ARRAY_HOLDER_V01_ID =
            (MIN_ID_2 + 270);
    
    /**
    class com.splicemachine.db.iapi.services.io.FormatableProperties
     */
    static public final int FORMATABLE_PROPERTIES_V01_ID =
            (MIN_ID_2 + 271);

    /**
    class com.splicemachine.db.iapi.services.io.FormatableIntHolder
     */
    static public final int FORMATABLE_INT_HOLDER_V01_ID =
            (MIN_ID_2 + 303);
    
    /**
    class com.splicemachine.db.iapi.services.io.FormatableLongHolder
     */
    static public final int FORMATABLE_LONG_HOLDER_V01_ID =
            (MIN_ID_2 + 329);

    /**
    class com.splicemachine.db.iapi.services.io.FormatableHashtable
     */
    static public final int FORMATABLE_HASHTABLE_V01_ID =
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
    static public final int SCHEMA_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 371);
    
    /**
    class com.splicemachine.db.impl.sql.catalog.ColumnDescriptorFinder
     */
    static public final int COLUMN_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 393);

    /**
    class com.splicemachine.db.impl.sql.catalog.SequenceDescriptorFinder
     */
    static public final int SEQUENCE_DESCRIPTOR_FINDER_V01_ID =
            (MIN_ID_2 + 472);

    static public final int PERM_DESCRIPTOR_FINDER_V01_ID = (MIN_ID_2 + 473);
        
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
    static public final int UNUSED_243 =
            (MIN_ID_2 + 243);
    
    /**
    ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    ||
    ||            DEPRECATED
    ||
    ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

    class com.splicemachine.db.impl.sql.catalog.OIDImpl
     */
    static public final int OIDIMPL_V01_ID =
            (MIN_ID_2 + 15);
    
    /**
        class com.splicemachine.db.catalog.types.BooleanTypeIdImpl
     */
    static public final int BOOLEAN_TYPE_ID_IMPL =
            (MIN_ID_2 + 16);

    /**
        class com.splicemachine.db.catalog.types.CharTypeIdImpl
     */
    static public final int CHAR_TYPE_ID_IMPL =
            (MIN_ID_2 + 17);
    
    /**
        class com.splicemachine.db.catalog.types.DoubleTypeIdImpl
     */
    static public final int DOUBLE_TYPE_ID_IMPL =
            (MIN_ID_2 + 18);
    
    /**
        class com.splicemachine.db.catalog.types.IntTypeIdImpl
     */
    static public final int INT_TYPE_ID_IMPL =
            (MIN_ID_2 + 19);
    
    /**
        class com.splicemachine.db.catalog.types.RealTypeIdImpl
     */
    static public final int REAL_TYPE_ID_IMPL =
            (MIN_ID_2 + 20);
    
    /**
        class com.splicemachine.db.catalog.types.RefTypeIdImpl
     */
    static public final int REF_TYPE_ID_IMPL =
            (MIN_ID_2 + 21);
    
    /**
        class com.splicemachine.db.catalog.types.SmallintTypeIdImpl
     */
    static public final int SMALLINT_TYPE_ID_IMPL =
            (MIN_ID_2 + 22);
    
    /**
        class com.splicemachine.db.catalog.types.LongintTypeIdImpl
     */
    static public final int LONGINT_TYPE_ID_IMPL =
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
    static public final int USERDEFINED_TYPE_ID_IMPL_V3 =
            (MIN_ID_2 + 264);
    /**
        class com.splicemachine.db.catalog.types.TypesImplInstanceGetter
     */
    static public final int DATE_TYPE_ID_IMPL =
            (MIN_ID_2 + 32);

    /**
        class com.splicemachine.db.catalog.types.TypesImplInstanceGetter
     */
    static public final int TIME_TYPE_ID_IMPL =
            (MIN_ID_2 + 33);
    /**
        class com.splicemachine.db.catalog.types.TypesImplInstanceGetter
     */
    static public final int TIMESTAMP_TYPE_ID_IMPL =
            (MIN_ID_2 + 34);

    /**
        class com.splicemachine.db.Database.Language.Execution.MinAggregator
     */
    static public final int AGG_MIN_V01_ID =
            (MIN_ID_2 + 153);

    /**
        class com.splicemachine.db.Database.Language.Execution.CountStarAggregator
     */
    static public final int AGG_COUNT_STAR_V01_ID =
            (MIN_ID_2 + 150);


    /**
        class com.splicemachine.db.catalog.types.VarcharTypeIdImpl
     */
    static public final int VARCHAR_TYPE_ID_IMPL =
            (MIN_ID_2 + 25);

    /**
        class com.splicemachine.db.impl.sql.catalog.ParameterDescriptorImpl
     */
    static public final int PARAMETER_DESCRIPTOR_V01_ID =
            (MIN_ID_2 + 26);

    /**
        class com.splicemachine.db.iapi.types.BitTypeId
     */
    static public final int BIT_TYPE_ID =
            (MIN_ID_2 + 27);

    /**
        class com.splicemachine.db.catalog.types.BitTypeIdImpl
     */
    static public final int BIT_TYPE_ID_IMPL =
            (MIN_ID_2 + 28);

    /**
        class com.splicemachine.db.iapi.types.VarbitTypeId
     */
    static public final int VARBIT_TYPE_ID =
            (MIN_ID_2 + 29);

    /**
        class com.splicemachine.db.iapi.types.VarbitTypeId
     */
    static public final int VARBIT_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 258);
    
    /**
            class com.splicemachine.db.catalog.types.VarbitTypeIdImpl
     */
    static public final int VARBIT_TYPE_ID_IMPL =
            (MIN_ID_2 + 30);


    /**
            class com.splicemachine.db.catalog.types.IndexDescriptorImpl
     */
    static public final int INDEX_DESCRIPTOR_IMPL_V02_ID =
            (MIN_ID_2 + 387);
    

    /**
        class com.splicemachine.db.iapi.types.TinyintTypeId
     */
    static public final int TINYINT_TYPE_ID =
            (MIN_ID_2 + 195);
    
    /**
            class com.splicemachine.db.catalog.types.TinyintTypeIdImpl
     */
    static public final int TINYINT_TYPE_ID_IMPL =
            (MIN_ID_2 + 196);

    /**
        class com.splicemachine.db.iapi.types.DecimalTypeId
     */
    static public final int DECIMAL_TYPE_ID =
            (MIN_ID_2 + 197);

    /**
        class com.splicemachine.db.iapi.types.DateTypeId
     */
    static public final int DATE_TYPE_ID =
            (MIN_ID_2 + 40);

    /**
        class com.splicemachine.db.iapi.types.TimeTypeId
     */
    static public final int TIME_TYPE_ID =
            (MIN_ID_2 + 35);

    /**
        class com.splicemachine.db.iapi.types.TimestampTypeId
     */
    static public final int TIMESTAMP_TYPE_ID =
                (MIN_ID_2 + 36);
        
    /**
        class com.splicemachine.db.catalog.types.DecimalTypeIdImpl
     */
    static public final int DECIMAL_TYPE_ID_IMPL =
            (MIN_ID_2 + 198);

    /**
        class com.splicemachine.db.catalog.types.LongvarcharTypeIdImpl
     */
    static public final int LONGVARCHAR_TYPE_ID_IMPL =
            (MIN_ID_2 + 231);

    /**
        class com.splicemachine.db.catalog.types.LongvarcharTypeIdImpl
     */
    static public final int CLOB_TYPE_ID_IMPL =
            (MIN_ID_2 + 446);

    /**
        class com.splicemachine.db.catalog.types.LongvarbitTypeIdImpl
            - does nto exist
     */
    static public final int LONGVARBIT_TYPE_ID_IMPL =
            (MIN_ID_2 + 233);

    /**
        class com.splicemachine.db.catalog.types.LongvarbitTypeIdImpl
        - does not exist, 
        but we do it the same way for BLOB as for Longvarbit...
     */
    static public final int BLOB_TYPE_ID_IMPL =
            (MIN_ID_2 + 442);

    /**
        class com.splicemachine.db.iapi.types.BitTypeId
     */
    static public final int BIT_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 253);
    
    /**
        class com.splicemachine.db.iapi.types.DecimalTypeId
     */
    static public final int DECIMAL_COMPILATION_TYPE_ID =
            (MIN_ID_2 + 254);
    
    /**
        class com.splicemachine.db.iapi.types.TinyintTypeId
     */
    static public final int TINYINT_COMPILATION_TYPE_ID =
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
    static public final int XML_TYPE_ID_IMPL =
            (MIN_ID_2 + 457);

    // 468 unused
    //        (MIN_ID_2 + 468);

    public static final int ROW_MULTISET_TYPE_ID_IMPL = 
            (MIN_ID_2 + 469);
    
    /******************************************************************
    **
    **      Execution MODULE CLASSES
    **
    ******************************************************************/

    /**
    class com.splicemachine.db.Database.Language.Execution.RenameConstantAction
    */
    static public final int RENAME_CONSTANT_ACTION_V01_ID   =
            (MIN_ID_2 + 390);

    /**
        class com.splicemachine.db.Database.Language.Execution.DeleteConstantAction
     */
    static public final int DELETE_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 37);

    /**
        class com.splicemachine.db.Database.Language.Execution.InsertConstantAction
     */
    static public final int INSERT_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 38);

    /**
        class com.splicemachine.db.Database.Language.Execution.UpdateConstantAction
     */
    static public final int UPDATABLE_VTI_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 375);

    /**
        class com.splicemachine.db.Database.Language.Execution.UpdateConstantAction
     */
    static public final int UPDATE_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 39);

    /**
     */
    static public final int UNUSED_2_204 =
            (MIN_ID_2 + 204);

    /**
        UNUSED
     */
    static public final int UNUSED_2_41 =
            (MIN_ID_2 + 41);
    
    /**
    class com.splicemachine.db.Database.Language.Execution.DropAliasConstantAction
    */
    static public final int UNUSED_2_42 =
            (MIN_ID_2 + 42);
    
    /**
    class com.splicemachine.db.Database.Language.Execution.CreateSchemaConstantAction
    */
    static public final int UNUSED_2_141    =
            (MIN_ID_2 + 141);
    
    /**
    */
    static public final int UNUSED_2_142    =
            (MIN_ID_2 + 142);
    
    /**
    class com.splicemachine.db.Database.Language.Execution.CreateViewConstantAction
    */
    static public final int UNUSED_2_143    =
            (MIN_ID_2 + 143);
    
    /**
    */
    static public final int UNUSED_2_144    =
            (MIN_ID_2 + 144);

    /**
        class com.splicemachine.db.Database.Language.Execution.ProviderInfo
     */
    static public final int PROVIDER_INFO_V01_ID =
            (MIN_ID_2 + 148);

    /**
        class com.splicemachine.db.Database.Language.Execution.AvgAggregator
     */
    static public final int AGG_AVG_V01_ID =
            (MIN_ID_2 + 149);

    /**
        class com.splicemachine.db.Database.Language.Execution.CountAggregator
     */
    static public final int AGG_COUNT_V01_ID =
            (MIN_ID_2 + 151);

    /**
        class com.splicemachine.db.Database.Language.Execution.MaxMinAggregator
     */
    static public final int AGG_MAX_MIN_V01_ID =
            (MIN_ID_2 + 152);

    /**
        class com.splicemachine.db.Database.Language.Execution.SumAggregator
     */
    static public final int AGG_SUM_V01_ID =
            (MIN_ID_2 + 154);

    /**
     class com.splicemachine.db.Database.Language.Execution.UserAggregatorAggregator
    */
    static public final int AGG_USER_ADAPTOR_V01_ID =
            (MIN_ID_2 + 323);

    /**
        class com.splicemachine.db.Database.Language.Execution.StatisticsConstantAction
    */
    static public final int STATISTICS_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 155);

    /**
        class com.splicemachine.db.Database.Language.Execution.LockTableConstantAction
    */
    static public final int UNUSED_2_275 =
            (MIN_ID_2 + 275);


    /**
    class com.splicemachine.db.Database.Language.Execution.CreateSPSConstantAction
    */
    static public final int UNUSED_2_221    =
            (MIN_ID_2 + 221);
    
    /**
    class com.splicemachine.db.Database.Language.Execution.CreateSPSConstantAction
    */
    static public final int UNUSED_2_222    =
            (MIN_ID_2 + 222);

    /**
    class com.splicemachine.db.Database.Language.Execution.AlterSPSConstantAction
    */
    static public final int ALTER_SPS_CONSTANT_ACTION_V01_ID        =
            (MIN_ID_2 + 229);

    /**
    class com.splicemachine.db.Database.Language.Execution.IndexColumnOrder
    */
    static public final int INDEX_COLUMN_ORDER_V01_ID       =
            (MIN_ID_2 + 218);

    /**
    class com.splicemachine.db.Database.Language.Execution.AggregateInfo
    */
    static public final int AGG_INFO_V01_ID =
            (MIN_ID_2 + 223);

    /**
    class com.splicemachine.db.Database.Language.Execution.AggregateInfoList
    */
    static public final int AGG_INFO_LIST_V01_ID    =
            (MIN_ID_2 + 224);

    /**
       class com.splicemachine.db.Database.Language.Execution.DeleteConstantAction
       This class is abstract so it doesn't need a format id!
     */
    static public final int WRITE_CURSOR_CONSTANT_ACTION_V01_ID =
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
    static public final int ADD_JAR_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 211);

    /**
      class com.splicemachine.db.impl.sql.execute.DropJarConstantAction;
     */
    static public final int DROP_JAR_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 212);

    /**
      class com.splicemachine.db.impl.sql.execute.ReplaceJarConstantAction;
     */
    static public final int REPLACE_JAR_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 213);

     /**
    class com.splicemachine.db.Database.Language.Execution.ConstraintInfo
     */
    static public final int CONSTRAINT_INFO_V01_ID  =
            (MIN_ID_2 + 278);

    /**
     */
    static public final int UNUSED_2_280 =
            (MIN_ID_2 + 280);

    /**
    class com.splicemachine.db.Database.Language.Execution.FKInfo
     */
    static public final int FK_INFO_V01_ID  =
            (MIN_ID_2 + 282);

    /**
     */
    static public final int UNUSED_2_289    =
            (MIN_ID_2 + 289);
    
    /**
    class com.splicemachine.db.impl.sql.execute.CreateTriggerConstantAction
     */
    static public final int CREATE_TRIGGER_CONSTANT_ACTION_V01_ID   =
            (MIN_ID_2 + 314);
    
    /**
    class com.splicemachine.db.impl.sql.execute.DropTriggerConstantAction
     */
    static public final int DROP_TRIGGER_CONSTANT_ACTION_V01_ID     =
            (MIN_ID_2 + 315);

    /**
    class com.splicemachine.db.impl.sql.execute.TriggerInfo
     */
    static public final int TRIGGER_INFO_V01_ID     =
            (MIN_ID_2 + 317);

    /**
    class com.splicemachine.db.impl.sql.execute.TransactionConstantAction
     */
    static public final int TRANSACTION_CONSTANT_ACTION_V01_ID      =
            (MIN_ID_2 + 318);

    /**
    class com.splicemachine.db.Database.Language.Execution.SetTriggersConstantAction
     */
    static public final int SET_TRIGGERS_CONSTANT_ACTION_V01_ID     =
            (MIN_ID_2 + 321);

    /**
        class com.splicemachine.db.Replication.Database.Language.Execution.RepSetTriggersConstantAction
     */
    static public final int REP_SET_TRIGGERS_CONSTANT_ACTION_V01_ID =
            (MIN_ID_2 + 322);

    /**
        class com.splicemachine.db.impl.sql.execute.RealLastIndexKeyScanStatistics
     */
    static public final int REAL_LAST_INDEX_KEY_SCAN_STATISTICS_IMPL_V01_ID =
            (MIN_ID_2 + 327);

    ////////////////////////////////////////////////////////////////////////////
    //
    // New versions of 2.0 Language ConstantActions, versioned in 3.0
    //
    ////////////////////////////////////////////////////////////////////////////


    /** class com.splicemachine.db.Database.Language.Execution.SetSchemaConstantAction */
    static public final int SET_SCHEMA_CONSTANT_ACTION_V02_ID                       = (MIN_ID_2 + 353);

    /** class com.splicemachine.db.Database.Language.Execution.SetTransactionIsolationConstantAction */
    static public final int SET_TRANSACTION_ISOLATION_CONSTANT_ACTION_V02_ID = (MIN_ID_2 + 354);

    /** class com.splicemachine.db.impl.sql.execute.ColumnInfo */
    static public final int COLUMN_INFO_V02_ID                      = (MIN_ID_2 + 358);

    /** class com.splicemachine.db.Database.Language.DependencySystem.Generic.ProviderInfo */
    static public final int PROVIDER_INFO_V02_ID                    = (MIN_ID_2 + 359);

    /** class com.splicemachine.db.impl.sql.execute.SavepointConstantAction */
    static public final int SAVEPOINT_V01_ID                      = (MIN_ID_2 + 452);

    /******************************************************************
    **
    **      LanguageInterface MODULE CLASSES
    **
    ******************************************************************/
    /**
    class com.splicemachine.db.impl.sql.GenericStorablePreparedStatement
     */
    static public final int STORABLE_PREPARED_STATEMENT_V01_ID      =
            (MIN_ID_2 + 225);
    
    /**
    class com.splicemachine.db.impl.sql.GenericResultDescription
     */
    static public final int GENERIC_RESULT_DESCRIPTION_V01_ID       =
            (MIN_ID_2 + 228);

    /**
    UNUSED
     */
    static public final int UNUSED_2_215    = (MIN_ID_2 + 215);

    /**
    class com.splicemachine.db.impl.sql.GenericTypeDescriptor
     */
    static public final int GENERIC_TYPE_DESCRIPTOR_V01_ID  =
            (MIN_ID_2 + 216);

    /**
    class com.splicemachine.db.impl.sql.GenericTypeId
     */
    static public final int GENERIC_TYPE_ID_V01_ID  =
            (MIN_ID_2 + 217);

    /**
    class com.splicemachine.db.impl.sql.CursorTableReference
     */
    static public final int CURSOR_TABLE_REFERENCE_V01_ID   =
            (MIN_ID_2 + 296);

    /**
    class com.splicemachine.db.impl.sql.CursorInfo
     */
    static public final int CURSOR_INFO_V01_ID      =
            (MIN_ID_2 + 297);

    /******************************************************************
    **
    **      ALIAS INFO CLASSES
    **
    ******************************************************************/

    /**
    class com.splicemachine.db.catalog.types.ClassAliasInfo
     */
    static public final int CLASS_ALIAS_INFO_V01_ID =
            (MIN_ID_2 + 310);

    /**
    class com.splicemachine.db.catalog.types.MethodAliasInfo
     */
    static public final int METHOD_ALIAS_INFO_V01_ID        =
            (MIN_ID_2 + 312);

    /**
    class com.splicemachine.db.catalog.types.WorkUnitAliasInfo
     */
    static public final int WORK_UNIT_ALIAS_INFO_V01_ID     =
            (MIN_ID_2 + 309);

    /**
    class com.splicemachine.db.catalog.types.UserAggregateAliasInfo
     */
    static public final int USER_AGGREGATE_ALIAS_INFO_V01_ID        =
            (MIN_ID_2 + 311);



    public static final int ROUTINE_INFO_V01_ID = (MIN_ID_2 + 451);
    public static final int SYNONYM_INFO_V01_ID = (MIN_ID_2 + 455);
    public static final int UDT_INFO_V01_ID = (MIN_ID_2 + 474);
    public static final int AGGREGATE_INFO_V01_ID = (MIN_ID_2 + 475);
    /**
     class com.splicemachine.db.Database.Language.Execution.WindowFunctionInfo
     */
    public static final int WINDOW_FUNCTION_INFO_V01_ID = (MIN_ID_2 + 476);
    /**
     class com.splicemachine.db.Database.Language.Execution.WindowFunctionInfoList
     */
    static public final int WINDOW_FUNCTION_INFO_LIST_V01_ID = (MIN_ID_2 + 477);

    /******************************************************************
    **
    **	DEFAULT INFO CLASSES
    **
    ******************************************************************/
            
    /**
    class com.splicemachine.db.catalog.types.DefaultInfoImpl
     */
    static public final int DEFAULT_INFO_IMPL_V01_ID =
            (MIN_ID_2 + 326);





    /**
    class com.splicemachine.db.impl.sql.GenericColumnDescriptor
     */
    static public final int GENERIC_COLUMN_DESCRIPTOR_V02_ID        =
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

    public static final int SQL_BOOLEAN_ID = 
            (MIN_ID_2 + 77);

    public static final int SQL_CHAR_ID = 
            (MIN_ID_2 + 78);

    public static final int SQL_DOUBLE_ID = 
            (MIN_ID_2 + 79);

    public static final int SQL_INTEGER_ID = 
            (MIN_ID_2 + 80);

    public static final int SQL_REAL_ID = 
            (MIN_ID_2 + 81);

    public static final int SQL_REF_ID = 
            (MIN_ID_2 + 82);

    public static final int SQL_SMALLINT_ID = 
            (MIN_ID_2 + 83);

    public static final int SQL_LONGINT_ID = 
            (MIN_ID_2 + 84);

    public static final int SQL_VARCHAR_ID = 
            (MIN_ID_2 + 85);

    //public static final int SQL_USERTYPE_ID = 
    //      (MIN_ID_2 + 86);

    //public static final int SQL_USERTYPE_ID_V2 = 
    //      (MIN_ID_2 + 266);

    public static final int SQL_USERTYPE_ID_V3 = 
            (MIN_ID_2 + 266);

    public static final int SQL_DATE_ID = 
            (MIN_ID_2 + 298);

    public static final int SQL_TIME_ID = 
            (MIN_ID_2 + 299);

    public static final int SQL_TIMESTAMP_ID = 
            (MIN_ID_2 + 31);

    public static final int SQL_BIT_ID = 
            (MIN_ID_2 + 87);

    public static final int SQL_VARBIT_ID = 
            (MIN_ID_2 + 88);

    public static final int SQL_TINYINT_ID = 
            (MIN_ID_2 + 199);

    public static final int SQL_DECIMAL_ID = 
            (MIN_ID_2 + 200);

    public static final int SQL_LONGVARCHAR_ID =
            (MIN_ID_2 + 235);

    public static final int SQL_CLOB_ID =
            (MIN_ID_2 + 447);

    public static final int SQL_LONGVARBIT_ID =
            (MIN_ID_2 + 234);

    public static final int SQL_BLOB_ID =
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
    public static final int XML_ID = 
            (MIN_ID_2 + 458);

    /******************************************************************
    ** 
    ** Access ids.
    **
    **
    **
    ******************************************************************/
    public static final int ACCESS_U8_V1_ID =
            (MIN_ID_2 + 89);

    public static final int ACCESS_HEAP_ROW_LOCATION_V1_ID =
            (MIN_ID_2 + 90);

    public static final int ACCESS_HEAP_V2_ID =
            (MIN_ID_2 + 91);

    public static final int ACCESS_B2I_V2_ID =
            (MIN_ID_2 + 92);

    public static final int ACCESS_FORMAT_ID =
            (MIN_ID_2 + 93);

    public static final int ACCESS_T_STRINGCOLUMN_ID =
            (MIN_ID_2 + 94);

    public static final int ACCESS_B2IUNDO_V1_ID =
            (MIN_ID_2 + 95);

    // Deleted as part of 7.2 rebrand project.

    /*
    public static final int ACCESS_CONGLOMDIR_V1_ID =
            (MIN_ID_2 + 96);
    */

    public static final int ACCESS_BTREE_LEAFCONTROLROW_V1_ID =
            (MIN_ID_2 + 133);

    public static final int ACCESS_BTREE_BRANCHCONTROLROW_V1_ID =
            (MIN_ID_2 + 134);

    public static final int ACCESS_SERIALIZABLEWRAPPER_V1_ID =
            (MIN_ID_2 + 202);

    public static final int ACCESS_B2I_STATIC_COMPILED_V1_ID =
            (MIN_ID_2 + 360);

    public static final int ACCESS_TREE_V1_ID =
            (MIN_ID_2 + 386);


    public static final int ACCESS_B2I_V3_ID =
            (MIN_ID_2 + 388);

    public static final int ACCESS_GISTUNDO_V1_ID =
            (MIN_ID_2 + 389);

    public static final int ACCESS_GIST_LEAFCONTROLROW_V1_ID =
            (MIN_ID_2 + 394);

    public static final int ACCESS_GIST_BRANCHCONTROLROW_V1_ID =
            (MIN_ID_2 + 395);

    public static final int STATISTICS_IMPL_V01_ID =
            (MIN_ID_2 + 397);

    public static final int UPDATE_STATISTICS_CONSTANT_ACTION_ID =
            (MIN_ID_2 +     398);

    public static final int DROP_STATISTICS_CONSTANT_ACTION_ID =
            (MIN_ID_2 + 399);

    public static final int ACCESS_GIST_RTREE_V1_ID =
            (MIN_ID_2 + 400);

    public static final int ACCESS_T_RECTANGLE_ID =
            (MIN_ID_4 + 34);

    public static final int ACCESS_T_INTCOL_V1_ID =      
            (MIN_ID_4 + 4);

    public static final int ACCESS_B2I_V4_ID =
            (MIN_ID_2 + 466);

    public static final int ACCESS_HEAP_V3_ID =
            (MIN_ID_2 + 467);

    public static final int ACCESS_B2I_V5_ID = 
            (MIN_ID_2 + 470);
    /******************************************************************
    **
    ** PropertyConglomerate
    ** 
    ** 
    ** 
    ******************************************************************/
    /** class com.splicemachine.db.impl.store.access.PropertyConglomerate */
      
    static public final int PC_XENA_VERSION_ID =
            (MIN_ID_2 + 15);


    /******************************************************************
    **
    ** Raw Store Log operation Ids
    **
    **
    **
    ******************************************************************/

    /* com.splicemachine.db.impl.store.raw.data.ChainAllocPageOperation */
    public static final int LOGOP_CHAIN_ALLOC_PAGE = 
            (MIN_ID_2 + 97);

    /* com.splicemachine.db.impl.store.raw.xact.BeginXact */
    public static final int LOGOP_BEGIN_XACT = 
            (MIN_ID_2 + 169);

    /* com.splicemachine.db.impl.store.raw.log.CheckpointOperation */
    public static final int LOGOP_CHECKPOINT =
            (MIN_ID_2 + 263);

    /* com.splicemachine.db.impl.store.raw.data.ContainerOperation */
    /* creating, dropping, removing container */
    public static final int LOGOP_CONTAINER = 
            (MIN_ID_2 + 242);

    /* com.splicemachine.db.impl.store.raw.data.DeleteOperation */
    public static final int LOGOP_DELETE = 
            (MIN_ID_2 + 101);

    /* com.splicemachine.db.impl.store.raw.xact.EndXact */
    public static final int LOGOP_END_XACT = 
            (MIN_ID_2 + 102);

    /* com.splicemachine.db.impl.store.raw.data.InsertOperation */
    public static final int LOGOP_INSERT = 
            (MIN_ID_2 + 103);

    /* com.splicemachine.db.impl.store.raw.data.LogicalUndoOperation */
    public static final int LOGOP_PAGE_LOGICAL_UNDO = 
            (MIN_ID_2 + 104);

    /* com.splicemachine.db.impl.store.raw.data.PhysicalUndoOperation */
    public static final int LOGOP_PAGE_PHYSICAL_UNDO = 
            (MIN_ID_2 + 105);

    /* com.splicemachine.db.impl.store.raw.data.PurgeOperation */
    public static final int LOGOP_PURGE = 
            (MIN_ID_2 + 106);

    /* com.splicemachine.db.impl.store.raw.data.ContainerUndoOperation */
    public static final int LOGOP_CONTAINER_UNDO = 
            (MIN_ID_2 + 107);

    /* com.splicemachine.db.impl.store.raw.data.UpdateOperation */
    public static final int LOGOP_UPDATE =
            (MIN_ID_2 + 108);

    /* com.splicemachine.db.impl.store.raw.data.UpdateFieldOperation */
    public static final int LOGOP_UPDATE_FIELD =
            (MIN_ID_2 + 109);

    /* com.splicemachine.db.impl.store.raw.data.CopyRowsOperation */
    public static final int LOGOP_COPY_ROWS = 
            (MIN_ID_2 + 210);

    /* com.splicemachine.db.impl.store.raw.data.AllocPageOperation */
    public static final int LOGOP_ALLOC_PAGE = 
            (MIN_ID_2 + 111);

    /*com.splicemachine.db.impl.store.raw.data.InitPageOperation */
    public static final int LOGOP_INIT_PAGE =
            (MIN_ID_2 + 241);

    /* com.splicemachine.db.impl.store.raw.data.InvalidatePageOperation */
    public static final int LOGOP_INVALIDATE_PAGE =
            (MIN_ID_2 + 113);

    /* com.splicemachine.db.impl.store.raw.data.SetReservedSpaceOperation */
    public static final int LOGOP_SET_RESERVED_SPACE = 
            (MIN_ID_2 + 287);

    /* com.splicemachine.db.impl.store.raw.data.RemoveFileOperation */
    public static final int LOGOP_REMOVE_FILE =
            (MIN_ID_2 + 291);

    /* com.splicemachine.db.impl.store.raw.log.ChecksumOperation */
    public static final int LOGOP_CHECKSUM =
            (MIN_ID_2 + 453);

    /* com.splicemachine.db.impl.store.raw.data.CompressSpacePageOperation10_2 */
    public static final int LOGOP_COMPRESS10_2_SPACE =
            (MIN_ID_2 + 454);

    /* com.splicemachine.db.impl.store.raw.data.CompressSpacePageOperation */
    public static final int LOGOP_COMPRESS_SPACE =
            (MIN_ID_2 + 465);

    /* com.splicemachine.db.impl.store.raw.data.EncryptContainerOperation */
    public static final int LOGOP_ENCRYPT_CONTAINER =
            (MIN_ID_2 + 459);

    /* com.splicemachine.db.impl.store.raw.data.EncryptContainerUndoOperation */
    public static final int LOGOP_ENCRYPT_CONTAINER_UNDO =
            (MIN_ID_2 + 460);

    /*******************************************************************
    **
    ** container types
    **
    ******************************************************************/
    /* com.splicemachine.db.impl.store.raw.data.FileContainer */
    public static final int RAW_STORE_SINGLE_CONTAINER_FILE = 
            (MIN_ID_2 + 116);

    /* com.splicemachine.db.impl.store.raw.data.StreamFileContainer */
    public static final int RAW_STORE_SINGLE_CONTAINER_STREAM_FILE = 
            (MIN_ID_2 + 290);

    /*******************************************************************
    **
    ** page types
    **
    **
    ******************************************************************/
    /* com.splicemachine.db.impl.store.raw.data.StoredPage */
    public static final int RAW_STORE_STORED_PAGE =
            (MIN_ID_2 + 117);

    /* com.splicemachine.db.impl.store.raw.data.AllocPage */
    public static final int RAW_STORE_ALLOC_PAGE =
            (MIN_ID_2 + 118);


    /*****************************************************************
    **
    ** Log files
    **
    **
    ******************************************************************/
    /* com.splicemachine.db.impl.store.raw.log.LogToFile */
    public static final int FILE_STREAM_LOG_FILE = 
            (MIN_ID_2 + 128);


    /*****************************************************************
    **
    ** Log record
    **
    ******************************************************************/
    /* com.splicemachine.db.impl.store.raw.log.LogRecord */
    public static final int LOG_RECORD = 
            (MIN_ID_2 + 129);

    /* com.splicemachine.db.impl.store.raw.log.LogCounter */
    public static final int LOG_COUNTER = 
            (MIN_ID_2 + 130);

    /******************************************************************
    **
    **  identifiers
    **
    ******************************************************************/
    /* com.splicemachine.db.impl.services.uuid.BasicUUID */
    public static final int BASIC_UUID = 
            (MIN_ID_2 + 131);

    /*
     *      Transaction Ids
     */

    /* com.splicemachine.db.impl.store.raw.xact.GlobalXactId */
    public static final int RAW_STORE_GLOBAL_XACT_ID_V20 = 
            (MIN_ID_2 + 132);

    /* com.splicemachine.db.impl.store.raw.xact.XactId */
    public static final int RAW_STORE_XACT_ID = 
            (MIN_ID_2 + 147);

    /* com.splicemachine.db.impl.store.raw.xact.XAXactId */
    public static final int RAW_STORE_GLOBAL_XACT_ID_NEW = 
            (MIN_ID_2 + 328);

    /*
     * Transaction table
     */
    /* com.splicemachine.db.impl.store.raw.xact.TransactionTableEntry */
    public static final int RAW_STORE_TRANSACTION_TABLE_ENTRY =
            (MIN_ID_2 + 261);

    /* com.splicemachine.db.impl.store.raw.xact.TransactionTable */
    public static final int RAW_STORE_TRANSACTION_TABLE =
            (MIN_ID_2 + 262);

            
    /******************************************************************
    **
    **  LocalDriver Formatables.
    **
    ******************************************************************/

    /* NOT USED = com.splicemachine.db.impl.jdbc.ExternalizableConnection */
    public static final int EXTERNALIZABLE_CONNECTION_ID = (MIN_ID_2 + 192);


    /******************************************************************
    **
    **      InternalUtils MODULE CLASSES
    **
    ******************************************************************/
    /* com.splicemachine.db.iapi.util.ByteArray */
    public static final int FORMATABLE_BYTE_ARRAY_V01_ID = (MIN_ID_2 + 219);


   /******************************************************************
    **
    **  UDPATE MAX_ID_2 WHEN YOU ADD A NEW FORMATABLE
    **
    ******************************************************************/


    /*
     * Make sure this is updated when a new module is added
     */
    public static final int MAX_ID_2 =
    		(MIN_ID_2 + 477);

    // DO NOT USE 4 BYTE IDS ANYMORE
    static public final int MAX_ID_4 =
            (MIN_ID_4 + 34);
}
