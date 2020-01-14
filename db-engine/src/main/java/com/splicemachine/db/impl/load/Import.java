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

package com.splicemachine.db.impl.load;

import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.util.*;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.iapi.util.StringUtil;

/**
 * This class implements import of data from a URL into a table.
 * Import functions provided here in this class shouble be called through
 * Systement Procedures. Import uses VTI , which is supprted only through 
 * Systemem procedures mechanism. 
 */

public class Import extends ImportAbstract{

    private static  int                _importCounter;

    //
    // This hashtable stores Import instances, which keep the context needed
    // to correlate Derby errors with line numbers in the file that is being
    // imported. An importing thread will access this hashtable at the very
    // beginning and the very end of its run. We cannot use Hashmap
    // because different threads may simultaneously put and delete entries.
    //
    private static  Hashtable   _importers = new Hashtable();

    private String inputFileName;

	/**
	 * Constructior to Invoke Import from a select statement 
	 * @param inputFileName	 The URL of the ASCII file from which import will happen
	 * @exception Exception on error 
	 */
	public Import(String inputFileName, String columnDelimiter,
                  String characterDelimiter,  String codeset, 
                  int noOfColumnsExpected,  String columnTypes, 
                  boolean lobsInExtFile,
                  int importCounter,
                  String columnTypeNames, String udtClassNamesString ) throws SQLException 
	{
		try{
			this.inputFileName = inputFileName;
            this.noOfColumnsExpected = noOfColumnsExpected;
            this.tableColumnTypesStr = columnTypes;
            this.columnTypeNamesString = columnTypeNames;
            this.udtClassNamesString = udtClassNamesString;
			controlFileReader = new ControlInfo();
			controlFileReader.setControlProperties(characterDelimiter,
												   columnDelimiter, codeset);
            this.lobsInExtFile = lobsInExtFile;

            _importers.put(importCounter, this );
            
			doImport();

		}catch(Exception e)
		{
			throw importError(e);
		}
	}


	private void doImport() throws Exception
	{
		if (inputFileName == null)
			throw LoadError.dataFileNull();
		doAllTheWork();

	}

	
	/**
	 * SYSCS_IMPORT_TABLE  system Procedure from ij or from a Java application
	 * invokes  this method to perform import to a table from a file.
	 * @param connection	 The Derby database connection URL for the database containing the table
	 * @param schemaName	The name of the schema where table to import exists 
	 * @param tableName     Name of the Table the data has to be imported to.
	 * @param inputFileName Name of the file from which data has to be imported.
	 * @param columnDelimiter  Delimiter that seperates columns in the file
	 * @param characterDelimiter  Delimiter that is used to quiote non-numeric types
	 * @param tableDefinition     table definition for the data in the file
	 * @param lobsInExtFile true, if the lobs data is stored in an external file,
     *                      and the reference to it is stored in the main import file.
 	 * @exception SQLException on errors
	 */

	public static void importTable(Connection connection, String schemaName, 
                                   String tableName, String inputFileName,  
                                   String columnDelimiter, 
                                   String characterDelimiter,String tableDefinition,
                                   boolean lobsInExtFile)
		throws SQLException {


		performImport(connection,  schemaName,  null, //No columnList 
					  null , //No column indexes
					  tableName, inputFileName, columnDelimiter, 
					  characterDelimiter, tableDefinition, lobsInExtFile);
	}



		
	/**
	 * SYSCS_IMPORT_DATA  system Procedure from ij or from a Java application
	 * invokes  this method to perform import to a table from a file.
	 * @param connection	 The Derby database connection URL for the database containing the table
	 * @param schemaName	The name of the schema where table to import exists 
	 * @param tableName     Name of the Table the data has to be imported to.
	 * @param insertColumnList  Comma Seperated column name list to which data
	 *                          has to be imported from file.eg: 'c2,c2,'c3'.
	 * @param columnIndexes     Comma sepearted Lit Index of the columns in the file(first column
	                             starts at 1). eg: '3 ,4 , 5'
	 * @param inputFileName Name of the file from which data has to be imported.
	 * @param columnDelimiter  Delimiter that seperates columns in the file
	 * @param characterDelimiter  Delimiter that is used to quiote non-numeric types
	 * @param tableDefinition     table definition for the data in the file
	 * @param lobsInExtFile true, if the lobs data is stored in an external file,
     *                      and the reference is stored in the main import file.
 	 * @exception SQLException on errors
	 */
	public static void importData(Connection connection, String schemaName,
                                  String tableName, String insertColumnList, 
                                  String columnIndexes, String inputFileName, 
                                  String columnDelimiter, 
                                  String characterDelimiter,
                                  String tableDefinition,
                                  boolean lobsInExtFile)
		throws SQLException 
	{
		

			performImport(connection, schemaName, insertColumnList, columnIndexes,
                    tableName, inputFileName, columnDelimiter,
                    characterDelimiter, tableDefinition, lobsInExtFile);
	}


	/*
	 * This function creates and executes  SQL Insert statement that performs the 
	 * the import using VTI. 
	 * eg:
	 * insert into T1 select  (cast column1 as DECIMAL), (cast column2 as
	 * INTEGER)  from new com.splicemachine.db.impl.load.Import('extin/Tutor1.asc') as importvti;
	 *
	 */
    private static void performImport
        (Connection connection, 
         String schemaName, 
         String insertColumnList, 
         String columnIndexes,
         String tableName, 
         String inputFileName,  
         String  columnDelimiter, 
         String characterDelimiter, 
         String columnDefinitions,
         boolean lobsInExtFile)
        throws SQLException 
    {
        Integer     importCounter = bumpImportCounter();
        
        try {
            if (connection == null)
                throw LoadError.connectionNull();
            
            
            
            if (tableName == null)
                throw LoadError.entityNameMissing();
            
            
            ColumnInfo columnInfo = null;//new ColumnInfo(connection , schemaName ,
                                         //          tableName, insertColumnList,
                                         //          columnIndexes, columnDefinitions);

            String columnTypeNames = null;
            String udtClassNames = null;
            try {
                columnTypeNames = columnInfo.getColumnTypeNames();
                udtClassNames = columnInfo.getUDTClassNames();
            }
            catch (Throwable t)
            {
                throw formatImportError( (Import) _importers.get( importCounter ), inputFileName, t );
            }

            String importvti = "new " + "com.splicemachine.derby.vti.SpliceFileVTI" +
                    "(" +
                    quoteStringArgument(inputFileName) +
                    "," +
                    quoteStringArgument(characterDelimiter) +
                    "," +
                    quoteStringArgument(columnDelimiter) +
                    " )";
            
            // delimit the table and schema names with quotes.
            // because they might have been  created as quoted
            // identifiers(for example when reserved words are used, names are quoted)
            
            // Import procedures are to be called with case-senisitive names. 
            // Incase of delimited table names, they need to be passed as defined
            // and when they are not delimited, they need to be passed in upper
            // case, because all undelimited names are stored in the upper case 
            // in the database. 
            
            String entityName = IdUtil.mkQualifiedName(schemaName, tableName);
            

            String cNamesWithCasts = null;//columnInfo.getColumnNamesWithCasts();
            String insertColumnNames = columnInfo.getInsertColumnNames();
            if(insertColumnNames !=null)
                insertColumnNames = "(" + insertColumnNames + ") " ;
            else
                insertColumnNames = "";
            String insertSql = "INSERT INTO " + entityName +  insertColumnNames + 
                " SELECT " + cNamesWithCasts + " from " +
                importvti + " AS importvti (" + columnDefinitions +")";
            
            //prepare the import statement to hit any errors before locking the table
            PreparedStatement ips = connection.prepareStatement(insertSql);
            
            //execute the import operaton.
            try {
                ips.executeUpdate();
            }
            catch (Throwable t)
            {
                throw formatImportError( (Import) _importers.get( importCounter ), inputFileName, t );
            }
            ips.close();
        }
        finally
        {
            //
            // The importer was put into a hashtable so that we could look up
            // line numbers for error messages. The Import constructor put
            // the importer in the hashtable. Now garbage collect that entry.
            //
            _importers.remove( importCounter );
        }
    }

	/** virtual method from the abstract class
	 * @exception	Exception on error
	 */
	ImportReadData getImportReadData() throws Exception {
		return new ImportReadData(inputFileName, controlFileReader);
	}

    /*
     * Bump the import counter.
     *
     */
    private static  synchronized    int bumpImportCounter()
    {
        return ++_importCounter;
    }
    
    /*
     * Format a import error with line number
     *
     */
    private static  SQLException    formatImportError( Import importer, String inputFile, Throwable t )
    {
        int     lineNumber = -1;

        if ( importer != null ) { lineNumber = importer.getCurrentLineNumber(); }
        
        StandardException se = StandardException.newException
            ( SQLState.UNEXPECTED_IMPORT_ERROR, lineNumber, inputFile, t.getMessage() );
        se.initCause(t);

        return PublicAPI.wrapStandardException(se);
    }

    /**
     * Quote a string argument so that it can be used as a literal in an
     * SQL statement. If the string argument is {@code null} an SQL NULL token
     * is returned.
     *
     * @param string a string or {@code null}
     * @return the string in quotes and with proper escape sequences for
     * special characters, or "NULL" if the string is {@code null}
     */
    private static String quoteStringArgument(String string) {
        if (string == null) {
            return "NULL";
        }
        return StringUtil.quoteStringLiteral(string);
    }
}
