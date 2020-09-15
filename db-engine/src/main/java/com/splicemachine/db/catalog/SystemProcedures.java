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

package com.splicemachine.db.catalog;

import com.splicemachine.db.iapi.db.Factory;
import com.splicemachine.db.iapi.db.PropertyInfo;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.iapi.util.StringUtil;
import com.splicemachine.db.impl.jdbc.EmbedDatabaseMetaData;
import com.splicemachine.db.impl.jdbc.Util;
import com.splicemachine.db.impl.load.Import;
import com.splicemachine.db.impl.sql.execute.JarUtil;
import com.splicemachine.db.jdbc.InternalDriver;
import com.splicemachine.db.shared.common.reference.AuditEventType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import com.splicemachine.utils.StringUtils;

import java.security.AccessController;
import java.security.Policy;
import java.security.PrivilegedAction;
import java.sql.*;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.StringTokenizer;

/**
 * Some system built-in procedures, and help routines.  Now used for network server.
 * These procedures are built-in to the SYSIBM schema which match the DB2 SYSIBM procedures.
 * Currently information on those can be found at url:
 * ftp://ftp.software.ibm.com/ps/products/db2/info/vr8/pdf/letter/db2l2e80.pdf
 * <p>
 * <p>
 * Also used for builtin-routines, such as SYSFUN functions, when direct calls
 * into Java libraries cannot be made.
 */
public class SystemProcedures{


    private final static int SQL_BEST_ROWID=1;
    private final static int SQL_ROWVER=2;
    private final static String DRIVER_TYPE_OPTION="DATATYPE";
    private final static String ODBC_DRIVER_OPTION="'ODBC'";

    private static final Logger AUDITLOG =Logger.getLogger("splice-audit");

    // This token delimiter value is used to separate the tokens for multiple 
    // error messages.  This is used in DRDAConnThread
    /**
     * <code>SQLERRMC_MESSAGE_DELIMITER</code> When message argument tokes are sent,
     * this value separates the tokens for mulitiple error messages
     */
    public static final String SQLERRMC_MESSAGE_DELIMITER=new String(new char[]{(char)20,(char)20,(char)20});

    /**
     * Method used by Derby Network Server to get localized message (original call
     * from jcc.
     *
     * @param sqlcode   sqlcode, not used.
     * @param errmcLen  sqlerrmc length
     * @param sqlerrmc  sql error message tokens, variable part of error message (ie.,
     *                  arguments) plus messageId, separated by separator.
     * @param sqlerrp   not used
     * @param errd0     not used
     * @param errd1     not used
     * @param errd2     not used
     * @param errd3     not used
     * @param errd4     not used
     * @param errd5     not used
     * @param warn      not used
     * @param sqlState  5-char sql state
     * @param file      not used
     * @param localeStr client locale in string
     * @param msg       OUTPUT parameter, localized error message
     * @param rc        OUTPUT parameter, return code -- 0 for success
     */
    public static void SQLCAMESSAGE(int sqlcode,short errmcLen,String sqlerrmc,
                                    String sqlerrp,int errd0,int errd1,int errd2,
                                    int errd3,int errd4,int errd5,String warn,
                                    String sqlState,String file,String localeStr,
                                    String[] msg,int[] rc){
        int numMessages=1;


        // Figure out if there are multiple exceptions in sqlerrmc. If so get each one
        // translated and append to make the final result.
        for(int index=0;;numMessages++){
            if(sqlerrmc.indexOf(SQLERRMC_MESSAGE_DELIMITER,index)==-1)
                break;
            index=sqlerrmc.indexOf(SQLERRMC_MESSAGE_DELIMITER,index)+
                    SQLERRMC_MESSAGE_DELIMITER.length();
        }

        // Putting it here instead of prepareCall it directly is because inter-jar reference tool
        // cannot detect/resolve this otherwise
        if(numMessages==1)
            MessageService.getLocalizedMessage(sqlcode,errmcLen,sqlerrmc,sqlerrp,errd0,errd1,
                    errd2,errd3,errd4,errd5,warn,sqlState,file,
                    localeStr,msg,rc);
        else{
            int startIdx=0, endIdx;
            String sqlError;
            String[] errMsg=new String[2];
            for(int i=0;i<numMessages;i++){
                endIdx=sqlerrmc.indexOf(SQLERRMC_MESSAGE_DELIMITER,startIdx);
                if(i==numMessages-1)                // last error message
                    sqlError=sqlerrmc.substring(startIdx);
                else sqlError=sqlerrmc.substring(startIdx,endIdx);

                if(i>0){
                    /* Strip out the SQLState */
                    sqlState=sqlError.substring(0,5);
                    sqlError=sqlError.substring(6);
                    msg[0]+=" SQLSTATE: "+sqlState+": ";
                }

                MessageService.getLocalizedMessage(sqlcode,(short)sqlError.length(),sqlError,
                        sqlerrp,errd0,errd1,errd2,errd3,errd4,errd5,
                        warn,sqlState,file,localeStr,errMsg,rc);

                if(rc[0]==0)            // success
                {
                    if(i==0)
                        msg[0]=errMsg[0];
                    else msg[0]+=errMsg[0];    // append the new message
                }
                startIdx=endIdx+SQLERRMC_MESSAGE_DELIMITER.length();
            }
        }
    }

    /**
     * Get the default or nested connection corresponding to the URL
     * jdbc:default:connection. We do not use DriverManager here
     * as it is not supported in JSR 169. IN addition we need to perform
     * more checks for null drivers or the driver returing null from connect
     * as that logic is in DriverManager.
     *
     * @return The nested connection
     * @throws SQLException Not running in a SQL statement
     */
    private static Connection getDefaultConn() throws SQLException{
        InternalDriver id=InternalDriver.activeDriver();
        if(id!=null){
            Connection conn=id.connect("jdbc:default:connection",null);
            if(conn!=null)
                return conn;
        }
        throw Util.noCurrentConnection();
    }

    /**
     * Get the DatabaseMetaData for the current connection for use in
     * mapping the jcc SYSIBM.* calls to the Derby DatabaseMetaData methods
     *
     * @return The DatabaseMetaData object of the current connection
     */
    private static DatabaseMetaData getDMD() throws SQLException{
        Connection conn=getDefaultConn();
        return conn.getMetaData();
    }

    /**
     * Map SQLProcedures to EmbedDatabaseMetaData.getProcedures
     *
     * @param catalogName SYSIBM.SQLProcedures CatalogName varchar(128),
     * @param schemaName  SYSIBM.SQLProcedures SchemaName  varchar(128),
     * @param procName    SYSIBM.SQLProcedures ProcName    varchar(128),
     * @param options     SYSIBM.SQLProcedures Options     varchar(4000))
     * @param rs          output parameter, the resultset object containing
     *                    the result of getProcedures
     *                    If options contains the string 'DATATYPE='ODBC'', call the ODBC
     *                    version of this procedure.
     */
    public static void SQLPROCEDURES(String catalogName,String schemaName,String procName,
                                     String options,ResultSet[] rs) throws SQLException{
        rs[0]=isForODBC(options)
                ?((EmbedDatabaseMetaData)getDMD()).getProceduresForODBC(
                catalogName,schemaName,procName)
                :getDMD().getProcedures(catalogName,schemaName,procName);
    }

    /**
     * Map SQLFunctions to EmbedDatabaseMetaData.getFunctions
     *
     * @param catalogName SYSIBM.SQLFunctions CatalogName varchar(128),
     * @param schemaName  SYSIBM.SQLFunctions SchemaName  varchar(128),
     * @param funcName    SYSIBM.SQLFunctions ProcName    varchar(128),
     * @param options     SYSIBM.SQLFunctions Options     varchar(4000))
     *                    (not used)
     * @param rs          output parameter, the resultset object containing
     *                    the result of getFunctions
     */
    public static void SQLFUNCTIONS(String catalogName,
                                    String schemaName,
                                    String funcName,
                                    String options,
                                    ResultSet[] rs) throws SQLException{
        rs[0]=getDMD().
                getFunctions(catalogName,schemaName,funcName);
    }

    /**
     * Map SQLTables to EmbedDatabaseMetaData.getSchemas, getCatalogs,
     * getTableTypes and getTables, and return the result of the
     * DatabaseMetaData calls.
     * <p>
     * <p>JCC and DNC overload this method:
     * <ul>
     * <li>If options contains the string 'GETSCHEMAS=1',
     * call getSchemas()</li>
     * <li>If options contains the string 'GETSCHEMAS=2',
     * call getSchemas(String, String)</li>
     * <li>If options contains the string 'GETCATALOGS=1',
     * call getCatalogs()</li>
     * <li>If options contains the string 'GETTABLETYPES=1',
     * call getTableTypes()</li>
     * <li>otherwise, call getTables()</li>
     * </ul>
     *
     * @param catalogName SYSIBM.SQLTables CatalogName varchar(128),
     * @param schemaName  SYSIBM.SQLTables SchemaName  varchar(128),
     * @param tableName   SYSIBM.SQLTables TableName   varchar(128),
     * @param tableType   SYSIBM.SQLTables TableType   varchar(4000))
     * @param options     SYSIBM.SQLTables Options     varchar(4000))
     * @param rs          output parameter, the resultset object
     */
    public static void SQLTABLES(String catalogName,String schemaName,String tableName,
                                 String tableType,String options,ResultSet[] rs)
            throws SQLException{

        String optionValue=getOption("GETCATALOGS",options);
        if(optionValue!=null && optionValue.trim().equals("1")){
            rs[0]=getDMD().getCatalogs();
            return;
        }
        optionValue=getOption("GETTABLETYPES",options);
        if(optionValue!=null && optionValue.trim().equals("1")){
            rs[0]=getDMD().getTableTypes();
            return;
        }
        optionValue=getOption("GETSCHEMAS",options);
        if(optionValue!=null){
            optionValue=optionValue.trim();
            if(optionValue.equals("1")){
                rs[0]=getDMD().getSchemas();
                return;
            }
            if(optionValue.equals("2")){
                EmbedDatabaseMetaData edmd=(EmbedDatabaseMetaData)getDMD();
                rs[0]=edmd.getSchemas(catalogName,schemaName);
                return;
            }
        }


        String[] typeArray=null;
        if(tableType!=null){
            StringTokenizer st=new StringTokenizer(tableType,"',");
            typeArray=new String[st.countTokens()];
            int i=0;

            while(st.hasMoreTokens()){
                typeArray[i]=st.nextToken();
                i++;
            }
        }
        rs[0]=getDMD().getTables(catalogName,schemaName,tableName,typeArray);
    }

    /**
     * Map SQLForeignKeys to EmbedDatabaseMetaData.getImportedKeys, getExportedKeys, and getCrossReference
     *
     * @param pkCatalogName SYSIBM.SQLForeignKeys PKCatalogName varchar(128),
     * @param pkSchemaName  SYSIBM.SQLForeignKeys PKSchemaName  varchar(128),
     * @param pkTableName   SYSIBM.SQLForeignKeys PKTableName   varchar(128),
     * @param fkCatalogName SYSIBM.SQLForeignKeys FKCatalogName varchar(128),
     * @param fkSchemaName  SYSIBM.SQLForeignKeys FKSchemaName  varchar(128),
     * @param fkTableName   SYSIBM.SQLForeignKeys FKTableName   varchar(128),
     * @param options       SYSIBM.SQLForeignKeys Options       varchar(4000))
     * @param rs            output parameter, the resultset object
     *                      containing the result of the DatabaseMetaData calls
     *                      JCC overloads this method:
     *                      If options contains the string 'IMPORTEDKEY=1', call getImportedKeys
     *                      If options contains the string 'EXPORTEDKEY=1', call getExportedKeys
     *                      otherwise, call getCrossReference
     */
    public static void SQLFOREIGNKEYS(String pkCatalogName,String pkSchemaName,String pkTableName,
                                      String fkCatalogName,String fkSchemaName,String fkTableName,
                                      String options,ResultSet[] rs)
            throws SQLException{

        String exportedKeyProp=getOption("EXPORTEDKEY",options);
        String importedKeyProp=getOption("IMPORTEDKEY",options);

        if(importedKeyProp!=null && importedKeyProp.trim().equals("1"))
            rs[0]=getDMD().getImportedKeys(fkCatalogName,
                    fkSchemaName,fkTableName);
        else if(exportedKeyProp!=null && exportedKeyProp.trim().equals("1"))
            rs[0]=getDMD().getExportedKeys(pkCatalogName,
                    pkSchemaName,pkTableName);
        else
            //ODBC allows table name value 'null'. JDBC does not
            rs[0]=isForODBC(options)
                    ?((EmbedDatabaseMetaData)getDMD()).getCrossReferenceForODBC(
                    pkCatalogName,pkSchemaName,pkTableName,
                    fkCatalogName,fkSchemaName,fkTableName)
                    :getDMD().getCrossReference(
                    pkCatalogName,pkSchemaName,pkTableName,
                    fkCatalogName,fkSchemaName,fkTableName);
    }

    /**
     * Helper for SQLForeignKeys and SQLTables
     *
     * @param pattern String containing the option to search for
     * @param options String containing the options to search through
     * @return option    String containing the value for a given option
     */
    private static String getOption(String pattern,String options){
        if(options==null)
            return null;
        int start=options.lastIndexOf(pattern);
        if(start<0)  // not there
            return null;
        int valueStart=options.indexOf('=',start);
        if(valueStart<0)  // invalid options string
            return null;
        int valueEnd=options.indexOf(';',valueStart);
        if(valueEnd<0)  // last option
            return options.substring(valueStart+1);
        else
            return options.substring(valueStart+1,valueEnd);
    }

    /**
     * Map SQLProcedureCols to EmbedDatabaseMetaData.getProcedureColumns
     *
     * @param catalogName SYSIBM.SQLProcedureCols CatalogName varchar(128),
     * @param schemaName  SYSIBM.SQLProcedureCols SchemaName  varchar(128),
     * @param procName    SYSIBM.SQLProcedureCols ProcName    varchar(128),
     * @param paramName   SYSIBM.SQLProcedureCols ParamName   varchar(128),
     * @param options     SYSIBM.SQLProcedureCols Options     varchar(4000))
     * @param rs          output parameter, the resultset object containing
     *                    the result of getProcedureColumns
     *                    If options contains the string 'DATATYPE='ODBC'', call the ODBC
     *                    version of this procedure.
     */
    public static void SQLPROCEDURECOLS(String catalogName,String schemaName,String procName,
                                        String paramName,String options,ResultSet[] rs)
            throws SQLException{
        rs[0]=isForODBC(options)
                ?((EmbedDatabaseMetaData)getDMD()).getProcedureColumnsForODBC(
                catalogName,schemaName,procName,paramName)
                :getDMD().getProcedureColumns(catalogName,schemaName,procName,paramName);
    }

    /**
     * Map SQLFunctionParameters to
     * EmbedDatabaseMetaData.getFunctionColumns()
     *
     * @param catalogName SYSIBM.SQLFunctionParameters CatalogName
     *                    varchar(128),
     * @param schemaName  SYSIBM.SQLFunctionParameters SchemaName
     *                    varchar(128),
     * @param funcName    SYSIBM.SQLFunctionParameters FuncName
     *                    varchar(128),
     * @param paramName   SYSIBM.SQLFunctionParameters ParamName
     *                    varchar(128),
     * @param options     SYSIBM.SQLFunctionParameters Options
     *                    varchar(4000))
     * @param rs          output parameter, the resultset object containing the
     *                    result of getFunctionColumns().
     */
    public static void SQLFUNCTIONPARAMS(String catalogName,
                                         String schemaName,
                                         String funcName,
                                         String paramName,
                                         String options,
                                         ResultSet[] rs) throws SQLException{
        rs[0]=((EmbedDatabaseMetaData)getDMD()).
                getFunctionColumns(catalogName,schemaName,funcName,
                        paramName);
    }


    /**
     * Map SQLColumns to EmbedDatabaseMetaData.getColumns
     *
     * @param catalogName SYSIBM.SQLColumns CatalogName varchar(128),
     * @param schemaName  SYSIBM.SQLColumns SchemaName  varchar(128),
     * @param tableName   SYSIBM.SQLColumns TableName   varchar(128),
     * @param columnName  SYSIBM.SQLColumns ColumnName  varchar(128),
     * @param options     SYSIBM.SQLColumns Options     varchar(4000))
     *                    If options contains the string 'DATATYPE='ODBC'', call the ODBC
     *                    version of this procedure.
     * @param rs          output parameter, the resultset object containing
     *                    the result of getProcedures
     */
    public static void SQLCOLUMNS(String catalogName,String schemaName,String tableName,
                                  String columnName,String options,ResultSet[] rs)
            throws SQLException{
        rs[0]=isForODBC(options)
                ?((EmbedDatabaseMetaData)getDMD()).getColumnsForODBC(
                catalogName,schemaName,tableName,columnName)
                :getDMD().getColumns(catalogName,schemaName,tableName,columnName);
    }

    /**
     * Map SQLColPrivileges to EmbedDatabaseMetaData.getColumnPrivileges
     *
     * @param catalogName SYSIBM.SQLColPrivileges CatalogName varchar(128),
     * @param schemaName  SYSIBM.SQLColPrivileges SchemaName  varchar(128),
     * @param tableName   SYSIBM.SQLColPrivileges ProcName    varchar(128),
     * @param columnName  SYSIBM.SQLColPrivileges ColumnName  varchar(128),
     * @param options     SYSIBM.SQLColPrivileges Options     varchar(4000))
     * @param rs          output parameter, the resultset object containing
     *                    the result of getColumnPrivileges
     */
    public static void SQLCOLPRIVILEGES(String catalogName,String schemaName,String tableName,
                                        String columnName,String options,ResultSet[] rs)
            throws SQLException{
        rs[0]=getDMD().getColumnPrivileges(catalogName,schemaName,tableName,columnName);
    }

    /**
     * Map SQLTablePrivileges to EmbedDatabaseMetaData.getTablePrivileges
     *
     * @param catalogName SYSIBM.SQLTablePrivileges CatalogName varchar(128),
     * @param schemaName  SYSIBM.SQLTablePrivileges SchemaName  varchar(128),
     * @param tableName   SYSIBM.SQLTablePrivileges ProcName    varchar(128),
     * @param options     SYSIBM.SQLTablePrivileges Options     varchar(4000))
     * @param rs          output parameter, the resultset object containing
     *                    the result of getTablePrivileges
     */
    public static void SQLTABLEPRIVILEGES(String catalogName,String schemaName,String tableName,
                                          String options,ResultSet[] rs)
            throws SQLException{
        rs[0]=getDMD().getTablePrivileges(catalogName,schemaName,tableName);
    }

    /**
     * Map SQLPrimaryKeys to EmbedDatabaseMetaData.getPrimaryKeys
     *
     * @param catalogName SYSIBM.SQLPrimaryKeys CatalogName varchar(128),
     * @param schemaName  SYSIBM.SQLPrimaryKeys SchemaName  varchar(128),
     * @param tableName   SYSIBM.SQLPrimaryKeys TableName   varchar(128),
     * @param options     SYSIBM.SQLPrimaryKeys Options     varchar(4000))
     *                    If options contains the string 'DATATYPE='ODBC'', call the ODBC
     *                    version of this procedure.
     * @param rs          output parameter, the resultset object containing
     *                    the result of getPrimaryKeys
     */
    public static void SQLPRIMARYKEYS(String catalogName,String schemaName,String tableName,String options,ResultSet[] rs)
            throws SQLException{
        rs[0]=getDMD().getPrimaryKeys(catalogName,schemaName,tableName);
    }

    /**
     * Map SQLGetTypeInfo to EmbedDatabaseMetaData.getTypeInfo
     *
     * @param dataType SYSIBM.SQLGetTypeInfo DataType smallint,
     * @param options  SYSIBM.SQLGetTypeInfo Options  varchar(4000))
     *                 If options contains the string 'DATATYPE='ODBC'', call the ODBC
     *                 version of this procedure.
     * @param rs       output parameter, the resultset object containing the
     *                 result of getTypeInfo
     */
    public static void SQLGETTYPEINFO(short dataType,String options,ResultSet[] rs)
            throws SQLException{
        rs[0]=isForODBC(options)
                ?((EmbedDatabaseMetaData)getDMD()).getTypeInfoForODBC()
                :getDMD().getTypeInfo();
    }

    /**
     * Map SQLStatistics to EmbedDatabaseMetaData.getIndexInfo
     *
     * @param catalogName SYSIBM.SQLStatistics CatalogName varchar(128),
     * @param schemaName  SYSIBM.SQLStatistics SchemaName  varchar(128),
     * @param tableName   SYSIBM.SQLStatistics TableName   varchar(128),
     * @param unique      SYSIBM.SQLStatistics Unique      smallint; 0=SQL_INDEX_UNIQUE(0); 1=SQL_INDEX_ALL(1),
     * @param approximate SYSIBM.SQLStatistics Approximate smallint; 1=true; 0=false,
     * @param options     SYSIBM.SQLStatistics Options     varchar(4000))
     *                    If options contains the string 'DATATYPE='ODBC'', call the ODBC
     *                    version of this procedure.
     * @param rs          output parameter, the resultset object containing
     *                    the result of getIndexInfo
     */
    public static void SQLSTATISTICS(String catalogName,String schemaName,String tableName,
                                     short unique,short approximate,String options,ResultSet[] rs)
            throws SQLException{
        boolean boolUnique= unique == 0;
        boolean boolApproximate= approximate == 1;

        rs[0]=isForODBC(options)
                ?((EmbedDatabaseMetaData)getDMD()).getIndexInfoForODBC(
                catalogName,schemaName,tableName,boolUnique,boolApproximate)
                :getDMD().getIndexInfo(catalogName,schemaName,tableName,boolUnique,boolApproximate);
    }

    /**
     * Map SQLSpecialColumns to EmbedDatabaseMetaData.getBestRowIdentifier and getVersionColumns
     *
     * @param colType     SYSIBM.SQLSpecialColumns ColType     smallint,
     *                    where 1 means getBestRowIdentifier and 2 getVersionColumns was called.
     * @param catalogName SYSIBM.SQLSpecialColumns CatalogName varchar(128),
     * @param schemaName  SYSIBM.SQLSpecialColumns SchemaName  varchar(128),
     * @param tableName   SYSIBM.SQLSpecialColumns TableName   varchar(128),
     * @param scope       SYSIBM.SQLSpecialColumns Scope       smallint,
     * @param nullable    SYSIBM.SQLSpecialColumns Nullable    smallint; 0=false, 1=true,
     * @param options     SYSIBM.SQLSpecialColumns Options     varchar(4000))
     *                    If options contains the string 'DATATYPE='ODBC'', call the ODBC
     *                    version of this procedure.
     * @param rs          output parameter, the resultset object containing
     *                    the result of the DatabaseMetaData call
     */
    public static void SQLSPECIALCOLUMNS(short colType,String catalogName,String schemaName,String tableName,
                                         short scope,short nullable,String options,ResultSet[] rs)
            throws SQLException{

        boolean boolNullable= nullable == 1;
        if(colType==SQL_BEST_ROWID){
            rs[0]=isForODBC(options)
                    ?((EmbedDatabaseMetaData)getDMD()).getBestRowIdentifierForODBC(
                    catalogName,schemaName,tableName,scope,boolNullable)
                    :getDMD().getBestRowIdentifier(catalogName,schemaName,tableName,scope,boolNullable);
        }else // colType must be SQL_ROWVER
        {
            rs[0]=isForODBC(options)
                    ?((EmbedDatabaseMetaData)getDMD()).getVersionColumnsForODBC(
                    catalogName,schemaName,tableName)
                    :getDMD().getVersionColumns(catalogName,schemaName,tableName);
        }
    }

    /**
     * Map SQLUDTS to EmbedDatabaseMetaData.getUDTs
     *
     * @param catalogName     SYSIBM.SQLUDTS CatalogName          varchar(128),
     * @param schemaPattern   SYSIBM.SQLUDTS Schema_Name_Pattern  varchar(128),
     * @param typeNamePattern SYSIBM.SQLUDTS Type_Name_Pattern    varchar(128),
     * @param udtTypes        SYSIBM.SQLUDTS UDTTypes             varchar(128),
     * @param options         SYSIBM.SQLUDTS Options              varchar(4000))
     * @param rs              output parameter, the resultset object containing
     *                        the result of getUDTs, which will be empty
     */
    public static void SQLUDTS(String catalogName,String schemaPattern,String typeNamePattern,
                               String udtTypes,String options,ResultSet[] rs)
            throws SQLException{

        int[] types=null;

        if(udtTypes!=null && !udtTypes.isEmpty()){
            StringTokenizer tokenizer=new StringTokenizer(udtTypes," \t\n\t,");
            int udtTypeCount=tokenizer.countTokens();
            types=new int[udtTypeCount];
            String udtType="";
            try{
                for(int i=0;i<udtTypeCount;i++){
                    udtType=tokenizer.nextToken();
                    types[i]=Integer.parseInt(udtType);
                }
            }catch(NumberFormatException nfe){
                throw new SQLException("Invalid type, "+udtType+", passed to getUDTs.");
            }catch(NoSuchElementException nsee){
                throw new SQLException("Internal failure: NoSuchElementException in getUDTs.");
            }
        }
        rs[0]=getDMD().getUDTs(catalogName,schemaPattern,typeNamePattern,types);
    }

    /*
     *  Map SYSIBM.METADATA to appropriate EmbedDatabaseMetaData methods
     *  for now, using the sps in com.splicemachine.db.iapi.db.jdbc.datadictionary.metadata_net.properties
     *
     */
    public static void METADATA(ResultSet[] rs)
            throws SQLException{
        rs[0]=((EmbedDatabaseMetaData)getDMD()).getClientCachedMetaData();
    }

    /**
     * Helper for ODBC metadata calls.
     *
     * @param options String containig the options to search through.
     * @return True if options contain ODBC indicator; false otherwise.
     */
    private static boolean isForODBC(String options){

        String optionValue=getOption(DRIVER_TYPE_OPTION,options);
        return ((optionValue!=null) && optionValue.toUpperCase().equals(ODBC_DRIVER_OPTION));

    }

    /**
     * Set/delete the value of a property of the database in current connection.
     * <p>
     * Will be called as SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY.
     *
     * @param key   The property key.
     * @param value The new value, if null the property is deleted.
     * @throws StandardException Standard exception policy.
     **/
    public static void SYSCS_SET_DATABASE_PROPERTY(
            String key,
            String value)
            throws SQLException{
        // Need to elevate the transaction to make it writable. Otherwise,
        // the procedure will fail in Splice when we write to HBase.
        // Ideally we would pass in a 'real' conglomerate id here,
        // but the fixed string is fine for cases like this at the Derby level.
        // See SpliceDataDictionary ("dictionary"), SpliceAccessManager ("boot"),
        // StatisticsAdmin ("statistics") and many others for comparable examples.
        LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
        TransactionController tc=lcc.getTransactionExecute();
        try{
            tc.elevate("dbprops");
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
        PropertyInfo.setDatabaseProperty(key,value);
    }

    /**
     * Get the value of a property of the database in current connection.
     * <p>
     * Will be called as SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY.
     *
     * @param key The property key.
     * @throws StandardException Standard exception policy.
     **/
    public static String SYSCS_GET_DATABASE_PROPERTY(
            String key)
            throws SQLException{
        LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();

        try{
            return PropertyUtil.getDatabaseProperty(lcc.getTransactionExecute(),key);
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Update the statistics for
     * 1)all the indexes or
     * 2)a specific index on a table.
     * <p>
     * Calls either
     * "alter table tablename all update statistics " sql
     * or
     * "alter table tablename update statistics indexname" sql
     * This routine will be called when an application calls:
     * SYSCS_UTIL.SYSCS_UPDATE_STATISTICS
     * <p>
     *
     * @param schemaname schema name of the index(es) whose statistics will
     *                   be updated. Must be non-null, no default is used.
     * @param tablename  table name of the index(es) whose statistics will
     *                   be updated. Must be non-null.
     * @param indexname  Can be null. If not null or emptry string then the
     *                   user wants to update the statistics for only this
     *                   index. If null, then update the statistics for all
     *                   the indexes for the given table name.
     * @throws SQLException
     **/
    public static void SYSCS_UPDATE_STATISTICS(
            String schemaname,
            String tablename,
            String indexname)
            throws SQLException{
        String escapedSchema=IdUtil.normalToDelimited(schemaname);
        String escapedTableName=IdUtil.normalToDelimited(tablename);
        String query="alter table "+escapedSchema+"."+escapedTableName;
        if(indexname==null)
            query=query+" all update statistics ";
        else
            query=query+" update statistics "+IdUtil.normalToDelimited(indexname);
        try (Connection conn=getDefaultConn()) {
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.executeUpdate();
            }
        }
    }

    /**
     * Drop the statistics for
     * 1)all the indexes or
     * 2)a specific index on a table.
     *
     * @param schemaname schema name of the table/index(es) whose
     *                   statistics will be dropped. Must be non-null,
     *                   no default is used.
     * @param tablename  table name of the index(es) whose statistics will
     *                   be dropped. Must be non-null.
     * @param indexname  Can be null. If not null or emptry string then the
     *                   user wants to drop the statistics for only this
     *                   index. If null, then drop the statistics for all
     *                   the indexes for the given table name.
     * @throws SQLException
     */
    public static void SYSCS_DROP_STATISTICS(
            String schemaname,
            String tablename,
            String indexname)
            throws SQLException{
        // SYSCS_DROP_STATISTICS is supported in Derby but is not valid for Splice,
        // Rather than let the user think it was successful, we throw an exception
        // explicitly indicating this is not supported. When we choose to implement
        // the functionality, we can either repurpose this stored proc, or create a
        // different interface for it instead.
        throw new UnsupportedOperationException(
                StandardException.newException(SQLState.SPLICE_NOT_IMPLEMENTED,"SYSCS_DROP_STATISTICS")
        );

//        String escapedSchema = IdUtil.normalToDelimited(schemaname);
//        String escapedTableName = IdUtil.normalToDelimited(tablename);
//        String query = "alter table " + escapedSchema + "." + escapedTableName;
//        if (indexname == null)
//        	query = query + " all drop statistics ";
//        else
//        	query = query + " statistics drop " + IdUtil.normalToDelimited(indexname);
//        Connection conn = getDefaultConn();
//
//        PreparedStatement ps = conn.prepareStatement(query);
//        ps.executeUpdate();
//        ps.close();
//
//        conn.close();
    }

    /**
     * Compress the table.
     * <p>
     * Calls the "alter table compress {sequential}" sql.  This syntax
     * is not db2 compatible so it mapped by a system routine.  This
     * routine will be called when an application calls:
     * <p>
     * SYSCS_UTIL.SYSCS_COMPRESS_TABLE
     * <p>
     *
     * @param schema     schema name of the table to compress.  Must be
     *                   non-null, no default is used.
     * @param tablename  table name of the table to compress.  Must be
     *                   non-null.
     * @param sequential if non-zero then rebuild indexes sequentially,
     *                   if 0 then rebuild all indexes in parallel.
     * @throws StandardException Standard exception policy.
     **/
    public static void SYSCS_COMPRESS_TABLE(
            String schema,
            String tablename,
            short sequential)
            throws SQLException{

        String escapedSchema=IdUtil.normalToDelimited(schema);
        String escapedTableName=IdUtil.normalToDelimited(tablename);
        String query=
                "alter table "+escapedSchema+"."+escapedTableName+
                        " compress"+(sequential!=0?" sequential":"");

        try (Connection conn=getDefaultConn()) {
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.executeUpdate();
            }
        }
    }

    /**
     * Freeze the database.
     * <p>
     * Call internal routine to freeze the database so that a backup
     * can be made.
     *
     * @throws StandardException Standard exception policy.
     **/
    public static void SYSCS_FREEZE_DATABASE()
            throws SQLException{
        Factory.getDatabaseOfConnection().freeze();
    }

    /**
     * Unfreeze the database.
     * <p>
     * Call internal routine to unfreeze the database, which was "freezed"
     * by calling SYSCS_FREEZE_DATABASE().
     * can be made.
     *
     * @throws StandardException Standard exception policy.
     **/
    public static void SYSCS_UNFREEZE_DATABASE()
            throws SQLException{
        Factory.getDatabaseOfConnection().unfreeze();
    }

    public static void SYSCS_CHECKPOINT_DATABASE()
            throws SQLException{
        Factory.getDatabaseOfConnection().checkpoint();
    }

    /**
     * Backup the database to a backup directory.
     * <p>
     * This procedure will throw error, if there are any unlogged
     * operation executed in the same transaction backup is started.
     * If there any unlogged operations in progess in other transaction, it
     * will wait until those transactions are completed before starting the backup.
     * <p>
     * Examples of unlogged operations include: create index and bulk insert.
     * Note that once the backup begins these operations will not block,
     * instead they are automatically converted into logged operations.
     *
     * @param backupDir the name of the directory where the backup should be
     *                  stored. This directory will be created if it
     *                  does not exist.
     * @throws StandardException thrown on error
     */
    public static void SYSCS_BACKUP_DATABASE(String backupDir)
            throws SQLException{
        Factory.getDatabaseOfConnection().backup(backupDir,true);
    }

    /**
     * @param restoreDir the name of the directory where the backup should be
     *                   stored. This directory will be created if it
     *                   does not exist.
     * @throws StandardException thrown on error
     */
    public static void SYSCS_RESTORE_DATABASE(String restoreDir)
            throws SQLException{
        Factory.getDatabaseOfConnection().restore(restoreDir,true);
    }


    /**
     * Backup the database to a backup directory.
     * <p>
     * This procedure will throw error, if there are any uncommitted unlogged
     * operation before stating the backup. It will not wait for the unlogged
     * operations to complete.
     * <p>
     * Examples of unlogged operations include: create index and bulk insert.
     * Note that once the backup begins these operations will not block,
     * instead they are automatically converted into logged operations.
     *
     * @param backupDir the name of the directory where the backup should be
     *                  stored. This directory will be created if it
     *                  does not exist.
     * @throws StandardException thrown on error
     */
    public static void SYSCS_BACKUP_DATABASE_NOWAIT(String backupDir)
            throws SQLException{
        Factory.getDatabaseOfConnection().backup(backupDir,false);
    }

    public static int SYSCS_CHECK_TABLE(
            String schema,
            String tablename)
            throws SQLException{
        boolean ret_val=
                com.splicemachine.db.iapi.db.ConsistencyChecker.checkTable(
                        schema,tablename);

        return (ret_val?1:0);
    }

    /**
     * Implementation of SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE().
     * <p>
     * Code which implements the following system procedure:
     * <p>
     * void SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(
     * IN SCHEMANAME        VARCHAR(128),
     * IN TABLENAME         VARCHAR(128),
     * IN PURGE_ROWS        SMALLINT,
     * IN DEFRAGMENT_ROWS   SMALLINT,
     * IN TRUNCATE_END      SMALLINT)
     * <p>
     * Use the SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE system procedure to reclaim
     * unused, allocated space in a table and its indexes. Typically, unused allocated
     * space exists when a large amount of data is deleted from a table, and there
     * have not been subsequent inserts to use the space freed by the deletes.
     * By default, Derby does not return unused space to the operating system. For
     * example, once a page has been allocated to a table or index, it is not
     * automatically returned to the operating system until the table or index is
     * destroyed. SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE allows you to return unused
     * space to the operating system.
     * <p>
     * This system procedure can be used to force 3 levels of in place compression
     * of a SQL table: PURGE_ROWS, DEFRAGMENT_ROWS, TRUNCATE_END.  Unlike
     * SYSCS_UTIL.SYSCS_COMPRESS_TABLE() all work is done in place in the existing
     * table/index.
     * <p>
     * Syntax:
     * SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(
     * IN SCHEMANAME        VARCHAR(128),
     * IN TABLENAME         VARCHAR(128),
     * IN PURGE_ROWS        SMALLINT,
     * IN DEFRAGMENT_ROWS   SMALLINT,
     * IN TRUNCATE_END      SMALLINT)
     * <p>
     * SCHEMANAME:
     * An input argument of type VARCHAR(128) that specifies the schema of the table. Passing a null will result in an error.
     * <p>
     * TABLENAME:
     * An input argument of type VARCHAR(128) that specifies the table name of the
     * table. The string must exactly match the case of the table name, and the
     * argument of "Fred" will be passed to SQL as the delimited identifier 'Fred'.
     * Passing a null will result in an error.
     * <p>
     * PURGE_ROWS:
     * If PURGE_ROWS is set to non-zero then a single pass is made through the table
     * which will purge committed deleted rows from the table.  This space is then
     * available for future inserted rows, but remains allocated to the table.
     * As this option scans every page of the table, it's performance is linearly
     * related to the size of the table.
     * <p>
     * DEFRAGMENT_ROWS:
     * If DEFRAGMENT_ROWS is set to non-zero then a single defragment pass is made
     * which will move existing rows from the end of the table towards the front
     * of the table.  The goal of the defragment run is to empty a set of pages
     * at the end of the table which can then be returned to the OS by the
     * TRUNCATE_END option.  It is recommended to only run DEFRAGMENT_ROWS, if also
     * specifying the TRUNCATE_END option.  This option scans the whole table and
     * needs to update index entries for every base table row move, and thus execution
     * time is linearly related to the size of the table.
     * <p>
     * TRUNCATE_END:
     * If TRUNCATE_END is set to non-zero then all contiguous pages at the end of
     * the table will be returned to the OS.  Running the PURGE_ROWS and/or
     * DEFRAGMENT_ROWS passes options may increase the number of pages affected.
     * This option itself does no scans of the table, so performs on the order of a
     * few system calls.
     * <p>
     * SQL example:
     * To compress a table called CUSTOMER in a schema called US, using all
     * available compress options:
     * call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('US', 'CUSTOMER', 1, 1, 1);
     * <p>
     * To quickly just return the empty free space at the end of the same table,
     * this option will run much quicker than running all phases but will likely
     * return much less space:
     * call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('US', 'CUSTOMER', 0, 0, 1);
     * <p>
     * Java example:
     * To compress a table called CUSTOMER in a schema called US, using all
     * available compress options:
     * <p>
     * CallableStatement cs = conn.prepareCall
     * ("CALL SYSCS_UTIL.SYSCS_COMPRESS_TABLE(?, ?, ?, ?, ?)");
     * cs.setString(1, "US");
     * cs.setString(2, "CUSTOMER");
     * cs.setShort(3, (short) 1);
     * cs.setShort(4, (short) 1);
     * cs.setShort(5, (short) 1);
     * cs.execute();
     * <p>
     * To quickly just return the empty free space at the end of the same table,
     * this option will run much quicker than running all phases but will likely
     * return much less space:
     * <p>
     * CallableStatement cs = conn.prepareCall
     * ("CALL SYSCS_UTIL.SYSCS_COMPRESS_TABLE(?, ?, ?, ?, ?)");
     * cs.setString(1, "US");
     * cs.setString(2, "CUSTOMER");
     * cs.setShort(3, (short) 0);
     * cs.setShort(4, (short) 0);
     * cs.setShort(5, (short) 1);
     * cs.execute();
     * <p>
     * <p>
     * It is recommended that the SYSCS_UTIL.SYSCS_COMPRESS_TABLE procedure is
     * issued in auto-commit mode.
     * Note: This procedure acquires an exclusive table lock on the table being compressed. All statement plans dependent on the table or its indexes are invalidated. For information on identifying unused space, see the Derby Server and Administration Guide.
     * <p>
     * TODO LIST:
     * o defragment requires table level lock in nested user transaction, which
     * will conflict with user lock on same table in user transaction.
     **/
    public static void SYSCS_INPLACE_COMPRESS_TABLE(
            String schema,
            String tablename,
            short purgeRows,
            short defragmentRows,
            short truncateEnd)
            throws SQLException{
        //Inplace compress let's the user call compress on VTI but it
        //is really a no-op. In order to avoid having to go throught
        //the ALTER TABLE code just for a no-op, we simply do the check
        //here and return if we are dealing with VTI.
        LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
        TransactionController tc=lcc.getTransactionExecute();

        try{
            DataDictionary data_dictionary=lcc.getDataDictionary();
            SchemaDescriptor sd=
                    data_dictionary.getSchemaDescriptor(schema,tc,true);
            TableDescriptor td=
                    data_dictionary.getTableDescriptor(tablename,sd,tc);

            if(td!=null && td.getTableType()==TableDescriptor.VTI_TYPE){
                return;
            }
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }

        //Send all the other inplace compress requests to ALTER TABLE
        //machinery
        String escapedSchema=IdUtil.normalToDelimited(schema);
        String escapedTableName=IdUtil.normalToDelimited(tablename);
        String query=
                "alter table "+escapedSchema+"."+escapedTableName+
                        " compress inplace"+(purgeRows!=0?" purge":"")
                        +(defragmentRows!=0?" defragment":"")
                        +(truncateEnd!=0?" truncate_end":"");

        try (Connection conn=getDefaultConn()) {
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.executeUpdate();
            }
        }
    }

	/*
	** SQLJ Procedures.
	*/

    /**
     * Install a jar file in the database.
     * <p>
     * SQLJ.INSTALL_JAR
     *
     * @param url    URL of the jar file to be installed in the database.
     * @param jar    SQL name jar will be installed as.
     * @param deploy Ignored.
     * @throws SQLException Error installing jar file.
     */
    public static void INSTALL_JAR(String url,String jar,int deploy)
            throws SQLException{

		try {
            LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
			String[] st = IdUtil.parseMultiPartSQLIdentifier(jar.trim());
			String schemaName;
			String sqlName;
            if (st.length == 1) {
				schemaName = lcc.getCurrentSchemaName();
				sqlName = st[0];
            }
            else {
                schemaName = st[0];
				sqlName = st[1];
			}
			checkJarSQLName(sqlName);
            JarUtil.install(lcc, schemaName, sqlName, url);
		} 
		catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}

    /**
     * Replace a jar file in the database.
     * <p>
     * SQLJ.REPLACE_JAR
     *
     * @param url URL of the jar file to be installed in the database.
     * @param jar SQL name of jar to be replaced.
     * @throws SQLException Error replacing jar file.
     */
    public static void REPLACE_JAR(String url,String jar)
            throws SQLException{

        try{

            LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();

            String[] st=IdUtil.parseMultiPartSQLIdentifier(jar.trim());

            String schemaName;
            String sqlName;

            if(st.length==1){
                schemaName=lcc.getCurrentSchemaName();
                sqlName=st[0];
            }else{
                schemaName=st[0];
                sqlName=st[1];
            }

            checkJarSQLName(sqlName);

            JarUtil.replace(lcc,
                    schemaName,sqlName,url);
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Remove a jar file from the database.
     *
     * @param jar      SQL name of jar to be replaced.
     * @param undeploy Ignored.
     * @throws SQLException Error removing jar file.
     */
    public static void REMOVE_JAR(String jar,int undeploy)
            throws SQLException{

        try{

            LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();

            String[] st=IdUtil.parseMultiPartSQLIdentifier(jar.trim());

            String schemaName;
            String sqlName;

            if(st.length==1){
                schemaName=lcc.getCurrentSchemaName();
                sqlName=st[0];
            }else{
                schemaName=st[0];
                sqlName=st[1];
            }

            checkJarSQLName(sqlName);

            JarUtil.drop(lcc,schemaName,sqlName);

        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    private static void checkJarSQLName(String sqlName)
            throws StandardException{

        // weed out a few special cases that cause problems.
        if((sqlName.isEmpty())
                || (sqlName.indexOf(':')!=-1)
                ){

            throw StandardException.newException(SQLState.ID_PARSE_ERROR);
        }
    }

    /**
     * Import  data from a given file to a table.
     * <p>
     * Will be called by system procedure as
     * SYSCS_IMPORT_TABLE(IN SCHEMANAME  VARCHAR(128),
     * IN TABLENAME    VARCHAR(128),  IN FILENAME VARCHAR(32672) ,
     * IN COLUMNDELIMITER CHAR(1),  IN CHARACTERDELIMITER CHAR(1) ,
     * IN CODESET VARCHAR(128), IN  REPLACE SMALLINT)
     *
     * @throws StandardException Standard exception policy.
     **/
    public static void SYSCS_IMPORT_TABLE(
            String schemaName,
            String tableName,
            String fileName,
            String columnDelimiter,
            String characterDelimiter,
            String columnDefinitions)
            throws SQLException{
        Connection conn=getDefaultConn();
        try{
            Import.importTable(conn,schemaName,tableName,fileName,
                    columnDelimiter,characterDelimiter,
                    columnDefinitions,false);
        }catch(SQLException se){
            rollBackAndThrowSQLException(conn,se);
        }
        //import finished successfull, commit it.
        conn.commit();
    }

    /**
     * issue a rollback when SQLException se occurs. If SQLException ouccurs when rollback,
     * the new SQLException will be added into the chain of se.
     */
    private static void rollBackAndThrowSQLException(Connection conn,
                                                     SQLException se) throws SQLException{
        try{
            conn.rollback();
        }catch(SQLException e){
            se.setNextException(e);
        }
        throw se;
    }

    /**
     * Import  data from a given file to a table. Data for large object
     * columns is in an external file, the reference to it is in the main
     * input file. Read the lob data from the external file using the
     * lob location info in the main import file.
     * <p>
     * Will be called by system procedure as
     * SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE(IN SCHEMANAME  VARCHAR(128),
     * IN TABLENAME    VARCHAR(128),  IN FILENAME VARCHAR(32672) ,
     * IN COLUMNDELIMITER CHAR(1),  IN CHARACTERDELIMITER CHAR(1) ,
     * IN CODESET VARCHAR(128), IN  REPLACE SMALLINT)
     *
     * @throws StandardException Standard exception policy.
     **/
    public static void SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE(
            String schemaName,
            String tableName,
            String fileName,
            String columnDelimiter,
            String characterDelimiter,
            String columnDefinitions)
            throws SQLException{
        try (Connection conn=getDefaultConn()) {
            try {
                Import.importTable(conn, schemaName, tableName, fileName,
                        columnDelimiter, characterDelimiter,
                        columnDefinitions,
                        true //lobs in external file
                );
            } catch (SQLException se) {
                rollBackAndThrowSQLException(conn, se);
            }
            //import finished successfull, commit it.
            conn.commit();
        }
    }


    /**
     * Import data from a given file into the specified table columns from the
     * specified columns in the file.
     * <p>
     * Will be called as
     * SYSCS_IMPORT_DATA (IN SCHEMANAME VARCHAR(128), IN TABLENAME VARCHAR(128),
     * IN INSERTCOLUMNLIST VARCHAR(32672), IN COLUMNINDEXES VARCHAR(32672),
     * IN FILENAME VARCHAR(32672), IN COLUMNDELIMITER CHAR(1),
     * IN CHARACTERDELIMITER CHAR(1), IN CODESET VARCHAR(128),
     * IN REPLACE SMALLINT)
     *
     * @throws StandardException Standard exception policy.
     **/
    public static void SYSCS_IMPORT_DATA(
            String schemaName,
            String tableName,
            String insertColumnList,
            String columnIndexes,
            String fileName,
            String columnDelimiter,
            String characterDelimiter,
            String columnDefinitions
    )
            throws SQLException, StandardException{
        try (Connection conn=getDefaultConn()) {
            Import.importData(conn, schemaName, tableName,
                    insertColumnList, columnIndexes, fileName,
                    columnDelimiter, characterDelimiter, columnDefinitions, false);
        }
    }


    /**
     * Import data from a given file into the specified table columns
     * from the  specified columns in the file. Data for large object
     * columns is in an  external file, the reference to it is in the
     * main input file. Read the lob data from the external file using
     * the lob location info in the main import file.
     * <p>
     * Will be called as
     * SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE(IN SCHEMANAME VARCHAR(128),
     * IN TABLENAME VARCHAR(128),
     * IN INSERTCOLUMNLIST VARCHAR(32672),
     * IN COLUMNINDEXES VARCHAR(32672),
     * IN FILENAME VARCHAR(32672), IN COLUMNDELIMITER CHAR(1),
     * IN CHARACTERDELIMITER CHAR(1), IN CODESET VARCHAR(128),
     * IN REPLACE SMALLINT)
     *
     * @throws StandardException Standard exception policy.
     **/
    public static void SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE(
            String schemaName,
            String tableName,
            String insertColumnList,
            String columnIndexes,
            String fileName,
            String columnDelimiter,
            String characterDelimiter,
            String columnDefinitions
    )
            throws SQLException{
        try (Connection conn=getDefaultConn()) {
            try {
                Import.importData(conn, schemaName, tableName,
                        insertColumnList, columnIndexes, fileName,
                        columnDelimiter, characterDelimiter,
                        columnDefinitions, true);
            } catch (SQLException se) {
                rollBackAndThrowSQLException(conn, se);
            }

            //import finished successfull, commit it.
            conn.commit();
        }
    }


    /**
     * Perform bulk insert using the specificed vti .
     * <p>
     * Will be called as
     * SYSCS_BULK_INSERT (IN SCHEMANAME VARCHAR(128), IN TABLENAME VARCHAR(128),
     * IN VTINAME VARCHAR(32672), IN VTIARG VARCHAR(32672))
     *
     * @throws StandardException Standard exception policy.
     **/
    @SuppressFBWarnings(value="SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING", justification = "no sql injection")
    public static void SYSCS_BULK_INSERT(
            String schemaName,
            String tableName,
            String vtiName,
            String vtiArg
    )
            throws SQLException{
        try (Connection conn=getDefaultConn()) {
            // Use default schema if schemaName is null. This isn't consistent
            // with the other procedures, as they would fail if schema was null.
            String entityName = IdUtil.mkQualifiedName(schemaName, tableName);

            String binsertSql =
                    "insert into " + entityName +
                            " --DERBY-PROPERTIES insertMode=bulkInsert \n" +
                            "select * from new " + IdUtil.normalToDelimited(vtiName) +
                            "(" +
                            // Ideally, we should have used parameter markers and setString(),
                            // but some of the VTIs need the parameter values when compiling
                            // the statement. Therefore, insert the strings into the SQL text.
                            StringUtil.quoteStringLiteral(schemaName) + ", " +
                            StringUtil.quoteStringLiteral(tableName) + ", " +
                            StringUtil.quoteStringLiteral(vtiArg) + ")" +
                            " as t";

            try (PreparedStatement ps = conn.prepareStatement(binsertSql)) {
                ps.executeUpdate();
            }
        }
    }

    /**
     * Reload the policy file.
     * <p>
     * System procedure called thusly:
     * <p>
     * SYSCS_UTIL.SYSCS_RELOAD_SECURITY_POLICY()
     **/
    public static void SYSCS_RELOAD_SECURITY_POLICY()
            throws SQLException{
        // If no security manager installed then there
        // is no policy to refresh. Calling Policy.getPolicy().refresh()
        // without a SecurityManager seems to lock in a policy with
        // no permissions thus ignoring the system property java.security.policy
        // when later installing a SecurityManager.
        if(System.getSecurityManager()==null)
            return;

        try{
            AccessController.doPrivileged(
                    new PrivilegedAction(){
                        public Object run(){
                            Policy.getPolicy().refresh();
                            return null;
                        }
                    });
        }catch(SecurityException se){
            throw Util.policyNotReloaded(se);
        }
    }

    /**
     * Method to return the constant PI.
     * SYSFUN.PI().
     *
     * @return PI
     */
    public static double PI(){
        return StrictMath.PI;
    }

    /**
     * Constant for natural log(10).
     */
    private static final double LOG10=StrictMath.log(10.0d);

    /**
     * Base 10 log function. SYSFUN.LOG10
     * Calculated by
     * <code>
     * log(value) / log(10)
     * </code>
     * where log is the natural log.
     */
    public static double LOG10(double value){
        return StrictMath.log(value)/LOG10;
    }

    /**
     * Cotangent function. SYSFUN.COT
     *
     * @return 1 / tan(x)
     * @see <a href="http://mathworld.wolfram.com/HyperbolicFunctions.html">HyperbolicFunctions</a>
     */
    public static double COT(double value){
        return 1.0/StrictMath.tan(value);
    }

    /**
     * Hyperbolic Cosine function. SYSFUN.COSH
     *
     * @return 1/2 (e^x + e^-x)
     * @see <a href="http://mathworld.wolfram.com/HyperbolicFunctions.html">HyperbolicFunctions</a>
     */
    public static double COSH(double value){
        return (StrictMath.exp(value)+StrictMath.exp(-value))/2.0;
    }

    /**
     * Hyperbolic Sine function. SYSFUN.SINH
     *
     * @return 1/2 (e^x - e^-x)
     * @see <a href="http://mathworld.wolfram.com/HyperbolicFunctions.html">HyperbolicFunctions</a>
     */
    public static double SINH(double value){
        return (StrictMath.exp(value)-StrictMath.exp(-value))/2.0;
    }

    /**
     * Hyperbolic Tangent function. SYSFUN.TANH
     *
     * @return (e^x - e^-x) / (e^x + e^-x)
     * @see <a href="http://mathworld.wolfram.com/HyperbolicFunctions.html">HyperbolicFunctions</a>
     */
    public static double TANH(double value){
        return (StrictMath.exp(value)-StrictMath.exp(-value))/
                (StrictMath.exp(value)+StrictMath.exp(-value));
    }

    /**
     * Method to return the sign of the given value.
     * SYSFUN.SIGN().
     *
     * @return 0, 1 or -1
     */
    public static int SIGN(double value){
        return value<0?-1:value>0?1:0;
    }

    /**
     * Pseudo-random number function.
     *
     * @return a random number
     */
    @SuppressFBWarnings(value="DMI_RANDOM_USED_ONLY_ONCE", justification = "FIX: DB-10207")
    public static double RAND(int seed){
        return (new Random(seed)).nextDouble();
    }

    /**
     * Method to round the given value to the given scale.
     * @param num
     * @param decimal
     * @return
     */
    public static double ROUND(double num, int decimal) {
        double scale = decimal < 18 ? Math.pow(10, decimal * 1.0) : Math.pow(10,18);
        return (Math.round(num * scale) / scale);
    }


    /**
     * Recompile all the invalid stored statements so they will not get recompiled when
     * executed next time around.
     */
    public static void SYSCS_RECOMPILE_INVALID_STORED_STATEMENTS()
            throws SQLException{
        try{
            LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
            DataDictionary dd=lcc.getDataDictionary();

            dd.recompileInvalidSPSPlans(lcc);
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Invalidate all the stored statements so they will get recompiled when
     * executed next time around.
     */
    public static void SYSCS_INVALIDATE_STORED_STATEMENTS()
            throws SQLException{
        try{
            LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
            DataDictionary dd=lcc.getDataDictionary();

            dd.invalidateAllSPSPlans(lcc);
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Update all the metadata stored statements so they will get refreshed from the metadata.properties file.
     */
    public static void SYSCS_UPDATE_METADATA_STORED_STATEMENTS()
            throws SQLException{
        try{
            LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
            TransactionController tc=lcc.getTransactionExecute();
            DataDictionary dd=lcc.getDataDictionary();

            /*
            ** Inform the data dictionary that we are about to write to it.
            ** There are several calls to data dictionary "get" methods here
            ** that might be done in "read" mode in the data dictionary, but
            ** it seemed safer to do this whole operation in "write" mode.
            **
            ** We tell the data dictionary we're done writing at the end of
            ** the transaction.
            */
            dd.startWriting(lcc);

            dd.updateMetadataSPSes(tc);
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Create a system stored procedure.
     * PLEASE NOTE:
     * This method is currently not used, but will be used when Splice Machine has a SYS_DEBUG schema available
     * with tools to debug and repair databases and data dictionaries.
     *
     * @param schemaName name of the system schema
     * @param procName   name of the system stored procedure
     * @throws SQLException
     */
    public static void SYSCS_CREATE_SYSTEM_PROCEDURE(String schemaName,String procName)
            throws SQLException{
        try{
            LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
            TransactionController tc=lcc.getTransactionExecute();
            DataDictionary dd=lcc.getDataDictionary();

            /*
            ** Inform the data dictionary that we are about to write to it.
            ** There are several calls to data dictionary "get" methods here
            ** that might be done in "read" mode in the data dictionary, but
            ** it seemed safer to do this whole operation in "write" mode.
            **
            ** We tell the data dictionary we're done writing at the end of
            ** the transaction.
            */
            dd.startWriting(lcc);

            dd.createSystemProcedure(schemaName,procName,tc);
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Drop a system stored procedure.
     * PLEASE NOTE:
     * This method is currently not used, but will be used when Splice Machine has a SYS_DEBUG schema available
     * with tools to debug and repair databases and data dictionaries.
     *
     * @param schemaName name of the system schema
     * @param procName   name of the system stored procedure
     * @throws SQLException
     */
    public static void SYSCS_DROP_SYSTEM_PROCEDURE(String schemaName,String procName)
            throws SQLException{
        try{
            LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
            TransactionController tc=lcc.getTransactionExecute();
            DataDictionary dd=lcc.getDataDictionary();

            /*
            ** Inform the data dictionary that we are about to write to it.
            ** There are several calls to data dictionary "get" methods here
            ** that might be done in "read" mode in the data dictionary, but
            ** it seemed safer to do this whole operation in "write" mode.
            **
            ** We tell the data dictionary we're done writing at the end of
            ** the transaction.
            */
            dd.startWriting(lcc);

            dd.dropSystemProcedure(schemaName,procName,tc);
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Empty as much of the cache as possible. It is not guaranteed
     * that the cache is empty after this call, as statements may be kept
     * by currently executing queries, activations that are about to be garbage
     * collected.
     */
    public static void SYSCS_EMPTY_STATEMENT_CACHE()
            throws SQLException{
        LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
        lcc.getDataDictionary().getDataDictionaryCache().clearStatementCache();
    }

    private static boolean hasSchema(Connection conn,String schemaName)
            throws SQLException{
        ResultSet rs=conn.getMetaData().getSchemas();
        boolean schemaFound=false;
        while(rs.next() && !schemaFound)
            schemaFound=schemaName.equals(rs.getString("TABLE_SCHEM"));
        rs.close();
        return schemaFound;
    }

    private static boolean hasTable(Connection conn,String schemaName,
                                    String tableName)
            throws SQLException{
        ResultSet rs=conn.getMetaData().getTables((String)null,
                schemaName,tableName,new String[]{"TABLE"});
        boolean tableFound=rs.next();
        rs.close();
        return tableFound;
    }

    /**
     * Create a new user.
     */
    public static void SYSCS_CREATE_USER
    (
            String userName,
            String password
    )
            throws SQLException{
        userName=normalizeUserName(userName);

        LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
        TransactionController tc=lcc.getTransactionExecute();

        String currentUser=lcc.getStatementContext().getSQLSessionContext().getCurrentUser();
        String ip = lcc.getClientIPAddress();
        boolean status = false;
        String reason = null;

        // the first credentials must be those of the DBO and only the DBO
        // can add them
        try{
            DataDictionary dd=lcc.getDataDictionary();
            String dbo=dd.getAuthorizationDatabaseOwner();

            if(!dbo.equals(userName)){
                if(dd.getUser(dbo)==null){
                    throw StandardException.newException(SQLState.DBO_FIRST);
                }
            }else    // we are trying to create credentials for the DBO
            {
                if(!dbo.equals(currentUser)){
                    throw StandardException.newException(SQLState.DBO_ONLY);
                }
            }
            addUser(userName,password,tc);


            // Create User Schema if one doesn't exists.
            // If there is already a schema then we will let the admin decide what he want to do
            // like granting privileges to that schema manually. Look at SPLICE-1030
            addSchema(userName, userName, lcc);
            status = true;

        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }catch (SQLException sqle){
            status = false;
            reason = sqle.getMessage();
            throw sqle;
        }
        finally {
            if (AUDITLOG.isInfoEnabled())
                AUDITLOG.info(StringUtils.logSpliceAuditEvent(currentUser,AuditEventType.CREATE_USER.name(),status,ip,lcc.getStatementContext().getStatementText(),reason));
        }

    }

    /**
     * Logic to add a new schema in the system. This is used  when we add a new user,
     * if the schema already exists do nothing, let the admin handle the privileges manually
     * if necessary. This schema is the default schema associated to each user.
     */

    public static void addSchema
    (
            String schemaName,
            String aid,
            LanguageConnectionContext lcc
    ) throws StandardException {
        DataDictionary dd=lcc.getDataDictionary();
        TransactionController tc=lcc.getTransactionExecute();
        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
        SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, lcc.getTransactionExecute(), false);

        // if the schema already, do nothing,  we already have a schema for the user
        // let the admin handle that
        if ((sd != null) && (sd.getUUID() != null)){
            return;
        }

        UUID tmpSchemaId = dd.getUUIDFactory().createUUID();
          /*
            ** Inform the data dictionary that we are about to write to it.
            ** There are several calls to data dictionary "get" methods here
            ** that might be done in "read" mode in the data dictionary, but
            ** it seemed safer to do this whole operation in "write" mode.
            **
            ** We tell the data dictionary we're done writing at the end of
            ** the transaction.
            */
        dd.startWriting(lcc);
        sd = ddg.newSchemaDescriptor(schemaName, aid, tmpSchemaId);
        dd.addDescriptor(sd, null, DataDictionary.SYSSCHEMAS_CATALOG_NUM, false, tc, false);

    }

    /**
     * Create a new user (this entry is called when bootstrapping the credentials of the DBO
     * at database creation time.
     */
    public static void addUser
    (
            String userName,
            String password,
            TransactionController tc
    )
            throws SQLException{
        try{
            LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
            DataDictionary dd=lcc.getDataDictionary();

            /*
            ** Inform the data dictionary that we are about to write to it.
            ** There are several calls to data dictionary "get" methods here
            ** that might be done in "read" mode in the data dictionary, but
            ** it seemed safer to do this whole operation in "write" mode.
            **
            ** We tell the data dictionary we're done writing at the end of
            ** the transaction.
            */
            dd.startWriting(lcc);
            UserDescriptor userDescriptor=makeUserDescriptor(dd,tc,userName,password);
            dd.addDescriptor(userDescriptor,null,DataDictionary.SYSUSERS_CATALOG_NUM,false,tc, false);

            // turn on NATIVE::LOCAL authentication
            if(dd.getAuthorizationDatabaseOwner().equals(userName)){
                //    tc.setProperty
                //        ( Property.AUTHENTICATION_PROVIDER_PARAMETER, Property.AUTHENTICATION_PROVIDER_NATIVE_LOCAL, true );
            }
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    private static UserDescriptor makeUserDescriptor
            (
                    DataDictionary dd,
                    TransactionController tc,
                    String userName,
                    String password
            )
            throws StandardException{
        DataDescriptorGenerator ddg=dd.getDataDescriptorGenerator();
        PasswordHasher hasher=dd.makePasswordHasher(tc.getProperties());

        if(hasher==null){
            throw StandardException.newException(SQLState.WEAK_AUTHENTICATION);
        }

        String hashingScheme=hasher.encodeHashingScheme();
        String hashedPassword=hasher.hashPasswordIntoString(userName,password);

        Timestamp currentTimestamp=new Timestamp((new java.util.Date()).getTime());

        return ddg.newUserDescriptor
                (userName,hashingScheme,hashedPassword.toCharArray(),currentTimestamp);
    }

    /**
     * Update all system schemas to have new authorizationId. This is needed
     * while upgrading pre-10.2 databases to 10.2 or later versions. From 10.2,
     * all system schemas would be owned by database owner's authorizationId.
     * This is also needed for Splice Machine when upgrading from the 0.5 beta
     * where there is no AnA to 1.0 where AnA is available for the first time.
     *
     * @param aid AuthorizationID of Database Owner
     * @param tc  TransactionController to use
     * @throws StandardException Thrown on failure
     */
    public static void updateSystemSchemaAuthorization(String aid,TransactionController tc) throws SQLException{
        try{
            LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
            DataDictionary dd=lcc.getDataDictionary();

			/*
			 ** Inform the data dictionary that we are about to write to it.
			 ** There are several calls to data dictionary "get" methods here
			 ** that might be done in "read" mode in the data dictionary, but
			 ** it seemed safer to do this whole operation in "write" mode.
			 **
			 ** We tell the data dictionary we're done writing at the end of
			 ** the transaction.
			 */
            dd.startWriting(lcc);
            // Change system schemas to be owned by aid
            dd.updateSystemSchemaAuthorization(aid,tc);
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Reset a user's password.
     */
    public static void SYSCS_RESET_PASSWORD
    (
            String userName,
            String password
    )
            throws SQLException{
        boolean status = false;
        LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
        String currentUser=lcc.getStatementContext().getSQLSessionContext().getCurrentUser();
        String ip = lcc.getClientIPAddress();
        String reason = null;
        try{
            resetAuthorizationIDPassword(normalizeUserName(userName),password);
            status = true;
        }catch (SQLException sqle) {
            status = false;
            reason = sqle.getMessage();
            throw sqle;
        }finally {
            if (AUDITLOG.isInfoEnabled())
                AUDITLOG.info(StringUtils.logSpliceAuditEvent(currentUser, AuditEventType.RESET_PASSWORD.name(),status,ip,lcc.getStatementContext().getStatementText(),reason));
        }

    }

    /**
     * Reset the password for an already normalized authorization id.
     */
    private static void resetAuthorizationIDPassword
    (
            String userName,
            String password
    )
            throws SQLException{
        try{
            LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
            DataDictionary dd=lcc.getDataDictionary();
            TransactionController tc=lcc.getTransactionExecute();

            checkLegalUser(dd,userName);
            
            /*
            ** Inform the data dictionary that we are about to write to it.
            ** There are several calls to data dictionary "get" methods here
            ** that might be done in "read" mode in the data dictionary, but
            ** it seemed safer to do this whole operation in "write" mode.
            **
            ** We tell the data dictionary we're done writing at the end of
            ** the transaction.
            */
            dd.startWriting(lcc);

            UserDescriptor userDescriptor=makeUserDescriptor(dd,tc,userName,password);

            dd.updateUser(userDescriptor,tc);

        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Change a user's password.
     */
    public static void SYSCS_MODIFY_PASSWORD
    (
            String password
    )
            throws SQLException{
        String currentUser=ConnectionUtil.getCurrentLCC().getStatementContext().getSQLSessionContext().getCurrentUser();

        boolean status = false;
        LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
        String ip = lcc.getClientIPAddress();
        String reason = null;
        try{
            resetAuthorizationIDPassword(currentUser,password);
            status = true;
        }catch (SQLException sqle){
            status = false;
            reason = sqle.getMessage();
            throw sqle;
        }finally {
            if (AUDITLOG.isInfoEnabled())
                AUDITLOG.info(StringUtils.logSpliceAuditEvent(currentUser,AuditEventType.MODIFY_PASSWORD.name(),status,ip,lcc.getStatementContext().getStatementText(),reason));
        }

    }

    /**
     * Drop a user.
     */
    public static void SYSCS_DROP_USER
    (
            String userName
    )
            throws SQLException{
        userName=normalizeUserName(userName);
        LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
        String currentUser=lcc.getStatementContext().getSQLSessionContext().getCurrentUser();
        String ip = lcc.getClientIPAddress();

        boolean status = false;
        String reason = null;
        try{
            DataDictionary dd=lcc.getDataDictionary();
            String dbo=dd.getAuthorizationDatabaseOwner();

            // you can't drop the credentials of the dbo
            if(dbo.equals(userName)){
                throw StandardException.newException(SQLState.CANT_DROP_DBO);
            }

            checkLegalUser(dd,userName);
            
            /*
            ** Inform the data dictionary that we are about to write to it.
            ** There are several calls to data dictionary "get" methods here
            ** that might be done in "read" mode in the data dictionary, but
            ** it seemed safer to do this whole operation in "write" mode.
            **
            ** We tell the data dictionary we're done writing at the end of
            ** the transaction.
            */
            dd.startWriting(lcc);

            dd.dropUser(userName,lcc.getTransactionExecute());
            status = true;

        }catch(StandardException se){
            status = false;
            reason = se.getMessage();
            throw PublicAPI.wrapStandardException(se);
        }finally {
            if (AUDITLOG.isInfoEnabled())
                AUDITLOG.info(StringUtils.logSpliceAuditEvent(currentUser, AuditEventType.DROP_USER.name(),status,ip,lcc.getStatementContext().getStatementText(),reason));
        }
    }

    /**
     * Raise an exception if the user doesn't exist. See commentary on DERBY-5648.
     */
    private static void checkLegalUser(DataDictionary dd,String userName)
            throws StandardException{
        if(dd.getUser(userName)==null){
            throw StandardException.newException(SQLState.NO_SUCH_USER);
        }
    }

    /**
     * Normalize the user name so that there is only one set of credentials
     * for a given authorization id.
     */
    private static String normalizeUserName(String userName)
            throws SQLException{
        try{
            return IdUtil.getUserAuthorizationId(userName);
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Peek at the current value of a sequence generator without advancing it.
     *
     * @param schemaName   The name of the schema holding the sequence.
     * @param sequenceName The name of the sequence in that schema.
     * @throws StandardException Standard exception policy.
     **/
    public static Long SYSCS_PEEK_AT_SEQUENCE(String schemaName,String sequenceName)
            throws SQLException{
        try{
            return ConnectionUtil.getCurrentLCC().getDataDictionary().peekAtSequence(schemaName,sequenceName);
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Set the connection level authorization for
     * a specific user - SYSCS_UTIL.SYSCS_SET_USER_ACCESS.
     *
     * @param userName             name of the user in its normal form (not a SQL identifier).
     * @param connectionPermission
     * @throws SQLException Error setting the permission
     */
    public static void SYSCS_SET_USER_ACCESS(String userName, String connectionPermission) throws SQLException{
        try{
            if(userName==null)
                throw StandardException.newException(SQLState.AUTH_INVALID_USER_NAME, (Object) null);

            String addListProperty;
            if(Property.FULL_ACCESS.equals(connectionPermission)){
                addListProperty=Property.FULL_ACCESS_USERS_PROPERTY;
            }else if(Property.READ_ONLY_ACCESS.equals(connectionPermission)){
                addListProperty=Property.READ_ONLY_ACCESS_USERS_PROPERTY;
            }else if(connectionPermission==null){
                // Remove from the lists but don't add back into any.
                addListProperty=null;
            }else
                throw StandardException.newException(SQLState.UU_UNKNOWN_PERMISSION,
                        connectionPermission);

            // Always remove from both lists to avoid any repeated
            // user on list errors.
            removeFromAccessList(Property.FULL_ACCESS_USERS_PROPERTY,
                    userName);
            removeFromAccessList(Property.READ_ONLY_ACCESS_USERS_PROPERTY,
                    userName);


            if(addListProperty!=null){
                String addList=SYSCS_GET_DATABASE_PROPERTY(addListProperty);
                SYSCS_SET_DATABASE_PROPERTY(addListProperty,
                        IdUtil.appendNormalToList(userName,addList));
            }

        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Utility method for SYSCS_SET_USER_ACCESS removes a user from
     * one of the access lists, driven by the property name.
     */
    private static void removeFromAccessList(
            String listProperty,String userName)
            throws SQLException, StandardException{
        String removeList=SYSCS_GET_DATABASE_PROPERTY(listProperty);
        if(removeList!=null){
            SYSCS_SET_DATABASE_PROPERTY(listProperty,
                    IdUtil.deleteId(userName,removeList));
        }
    }

}
