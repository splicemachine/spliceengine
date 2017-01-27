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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.sql.compile.TypeCompilerFactory;
import com.splicemachine.db.iapi.sql.compile.TypeCompiler;
import com.splicemachine.db.iapi.types.TypeId;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.reference.JDBC40Translation;

import java.sql.Types;

public class TypeCompilerFactoryImpl implements TypeCompilerFactory
{
        private static final String PACKAGE_NAME =
                        "com.splicemachine.db.impl.sql.compile.";

        // These are all the TypeCompilers that are stateless, so we can
        // use a single instance of each. Initialize all to null, and fault
        // them in.
        static TypeCompiler bitTypeCompiler;
        static TypeCompiler booleanTypeCompiler;
        static TypeCompiler charTypeCompiler;
        static TypeCompiler decimalTypeCompiler ;
        static TypeCompiler doubleTypeCompiler ;
        static TypeCompiler intTypeCompiler ;
        static TypeCompiler longintTypeCompiler ;
        static TypeCompiler longvarbitTypeCompiler ;
        static TypeCompiler longvarcharTypeCompiler ;
        static TypeCompiler realTypeCompiler ;
        static TypeCompiler smallintTypeCompiler ;
        static TypeCompiler tinyintTypeCompiler ;
        static TypeCompiler dateTypeCompiler ;
        static TypeCompiler timeTypeCompiler ;
        static TypeCompiler timestampTypeCompiler ;
        static TypeCompiler varbitTypeCompiler ;
        static TypeCompiler varcharTypeCompiler ;
        static TypeCompiler refTypeCompiler ;
        static TypeCompiler blobTypeCompiler ;
        static TypeCompiler clobTypeCompiler ;
        static TypeCompiler xmlTypeCompiler ;

        /**
         * Get a TypeCompiler corresponding to the given TypeId
         *
         * @param typeId        The TypeId to get a TypeCompiler for
         *
         * @return      The corresponding TypeCompiler
         */

        public TypeCompiler getTypeCompiler(TypeId typeId)
        {
                return staticGetTypeCompiler(typeId);
        }

        static TypeCompiler staticGetTypeCompiler(TypeId typeId)
        {
                String sqlTypeName;

                switch (typeId.getJDBCTypeId())
                {
                  case Types.BINARY:
                        return bitTypeCompiler =
                                        getAnInstance(PACKAGE_NAME + "BitTypeCompiler",
                                                                        bitTypeCompiler,
                                                                        typeId);

                  case Types.BIT:
                  case Types.BOOLEAN:
                        return booleanTypeCompiler =
                                        getAnInstance(PACKAGE_NAME + "BooleanTypeCompiler",
                                                                booleanTypeCompiler,
                                                                typeId);

                  case Types.CHAR:
                          sqlTypeName = typeId.getSQLTypeName();
                          return charTypeCompiler =
                              getAnInstance(PACKAGE_NAME + "CharTypeCompiler",
                                                      charTypeCompiler,
                                                      typeId);

                  case Types.NUMERIC:
                  case Types.DECIMAL:
                        return decimalTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "NumericTypeCompiler",
                                                                decimalTypeCompiler,
                                                                typeId);

                  case Types.DOUBLE:
                        return doubleTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "NumericTypeCompiler",
                                                                doubleTypeCompiler,
                                                                typeId);

                  case Types.INTEGER:
                        return intTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "NumericTypeCompiler",
                                                                intTypeCompiler,
                                                                typeId);

                  case Types.BIGINT:
                        return longintTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "NumericTypeCompiler",
                                                                longintTypeCompiler,
                                                                typeId);

                  case Types.BLOB:
                        return blobTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "LOBTypeCompiler",
                                                          blobTypeCompiler,
                                                          typeId);

                  case Types.LONGVARBINARY:
                        return longvarbitTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "BitTypeCompiler",
                                                          longvarbitTypeCompiler,
                                                          typeId);

                  case Types.CLOB:
                      sqlTypeName = typeId.getSQLTypeName();
                      return clobTypeCompiler =
                          getAnInstance(PACKAGE_NAME + "CLOBTypeCompiler",
                                        clobTypeCompiler,
                                        typeId);
                  case Types.LONGVARCHAR:
                          sqlTypeName = typeId.getSQLTypeName();
                          return longvarcharTypeCompiler =
                              getAnInstance(PACKAGE_NAME + "CharTypeCompiler",
                                                      longvarcharTypeCompiler,
                                                      typeId);

                  case Types.REAL:
                        return realTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "NumericTypeCompiler",
                                                                realTypeCompiler,
                                                                typeId);

                  case Types.SMALLINT:
                        return smallintTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "NumericTypeCompiler",
                                                                smallintTypeCompiler,
                                                                typeId);

                  case Types.TINYINT:
                    return tinyintTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "NumericTypeCompiler",
                                                                tinyintTypeCompiler,
                                                                typeId);

                  case Types.DATE:
                        return dateTypeCompiler =
                                        getAnInstance(PACKAGE_NAME + "DateTypeCompiler",
                                                                        dateTypeCompiler,
                                                                        typeId);

                  case Types.TIME:
                        return timeTypeCompiler =
                                        getAnInstance(PACKAGE_NAME + "TimeTypeCompiler",
                                                                        timeTypeCompiler,
                                                                        typeId);
                  case Types.TIMESTAMP:
                        return timestampTypeCompiler =
                                        getAnInstance(PACKAGE_NAME + "TimestampTypeCompiler",
                                                                        timestampTypeCompiler,
                                                                        typeId);
                  case Types.VARBINARY:
                        return varbitTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "BitTypeCompiler",
                                                                varbitTypeCompiler,
                                                                typeId);

                  case Types.VARCHAR:
                          sqlTypeName = typeId.getSQLTypeName();
                          return varcharTypeCompiler =
                              getAnInstance(PACKAGE_NAME + "CharTypeCompiler",
                                                      varcharTypeCompiler,
                                                      typeId);

                  case Types.JAVA_OBJECT:
                  case Types.REF:
                  case Types.OTHER:
                        if (typeId.isRefTypeId())
                        {
                                return refTypeCompiler = getAnInstance(
                                                                                        PACKAGE_NAME + "RefTypeCompiler",
                                                                                        refTypeCompiler,
                                                                                        typeId);
                        }
                        else
                        {
                                // Cannot re-use instances of user-defined type compilers,
                                // because they contain the class name
                                BaseTypeCompiler btc = new UserDefinedTypeCompiler();
                                btc.setTypeId(typeId);
                                return btc;
                        }

                  case JDBC40Translation.SQLXML:
                        return xmlTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "XMLTypeCompiler",
                                                                xmlTypeCompiler,
                                                                typeId);

                }

                if (SanityManager.DEBUG)
                {
                        SanityManager.THROWASSERT("Unexpected JDBC type id " +
                                                                                typeId.getJDBCTypeId() +
                                                                                " for typeId of class " +
                                                                                typeId.getClass().getName());
                }

                return null;
        }

        /**
         * Check whether the given TypeCompiler has been allocated yet.
         * If so, just return it, otherwise allocate a new instance
         * given its class.
         */
        private static TypeCompiler getAnInstance(String className,
                                                                TypeCompiler anInstance,
                                                                TypeId typeId)
        {
                if (anInstance == null)
                {
                        Exception exc = null;
                        Class typeCompilerClass = null;

                        try
                        {
                                typeCompilerClass = Class.forName(className);
                                anInstance  = (TypeCompiler) typeCompilerClass.newInstance();
                                ((BaseTypeCompiler) anInstance).setTypeId(typeId);
                        }
                        catch (ClassNotFoundException cnfe)
                        {
                                exc = cnfe;
                        }
                        catch (IllegalAccessException iae)
                        {
                                exc = iae;
                        }
                        catch (InstantiationException ie)
                        {
                                exc = ie;
                        }

                        if (SanityManager.DEBUG)
                        {
                                if (exc != null)
                                {
                                        SanityManager.THROWASSERT(
                                                "Exception " +
                                                exc +
                                                " while trying to get new instance of a " +
                                                typeCompilerClass.getName());
                                }
                        }
                }

                return anInstance;
        }
}
