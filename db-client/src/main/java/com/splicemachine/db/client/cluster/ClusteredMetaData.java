/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.cluster;


import com.splicemachine.db.client.am.Version;

import java.sql.*;
import java.util.logging.*;

/**
 * @author Scott Fines
 *         Date: 9/15/16
 */
class ClusteredMetaData implements DatabaseMetaData{
    private static final Logger LOGGER=Logger.getLogger(ClusteredMetaData.class.getName());

    private final Connection sourceConn;
    private final ClusteredConnManager connectionManager;
    private final int maxRetries;
    private final String connectionUrl;

    private RefCountedConnection conn;
    private DatabaseMetaData delegate;

    ClusteredMetaData(Connection sourceConn,String url,ClusteredConnManager connectionManager, int maxRetries){
        this.sourceConn = sourceConn;
        this.connectionManager=connectionManager;
        this.maxRetries = maxRetries;
        this.connectionUrl = url;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.allProceduresAreCallable();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }


    @Override
    public boolean allTablesAreSelectable() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.allTablesAreSelectable();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public String getURL() throws SQLException{
        return connectionUrl;
    }

    @Override
    public String getUserName() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                /*
                 * It doesn't seem likely for this to ever throw a network error (
                 * or an error at all, really), but this is safer, so may as well.
                 */
                return delegate.getUserName();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean isReadOnly() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.isReadOnly();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.nullsAreSortedHigh();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.nullsAreSortedLow();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.nullsAreSortedAtStart();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.nullsAreSortedAtEnd();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public String getDatabaseProductName() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getDatabaseProductName();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }


    @Override
    public String getDatabaseProductVersion() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getDatabaseProductVersion();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public String getDriverName() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getDriverName();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public String getDriverVersion() throws SQLException{
        return Version.getDriverVersion();
    }

    @Override
    public int getDriverMajorVersion(){
        return Version.getMajorVersion();
    }

    @Override
    public int getDriverMinorVersion(){
        return Version.getMinorVersion();
    }

    @Override
    public boolean usesLocalFiles() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.usesLocalFiles();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.usesLocalFilePerTable();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException{
        return false; //apparently all JDBC drivers do this, but we definitely do
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.storesUpperCaseIdentifiers();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.storesLowerCaseIdentifiers();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.storesMixedCaseIdentifiers();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsMixedCaseQuotedIdentifiers();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.storesUpperCaseQuotedIdentifiers();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.storesLowerCaseQuotedIdentifiers();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.storesMixedCaseQuotedIdentifiers();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getIdentifierQuoteString();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public String getSQLKeywords() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getSQLKeywords();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public String getNumericFunctions() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getNumericFunctions();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public String getStringFunctions() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getStringFunctions();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public String getSystemFunctions() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getSystemFunctions();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public String getTimeDateFunctions() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getTimeDateFunctions();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public String getSearchStringEscape() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getSearchStringEscape();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public String getExtraNameCharacters() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getExtraNameCharacters();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsAlterTableWithAddColumn();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsAlterTableWithDropColumn();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsColumnAliasing();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.nullPlusNonNullIsNull();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsConvert() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsConvert();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsConvert(int fromType,int toType) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsConvert(fromType, toType);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsTableCorrelationNames();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsDifferentTableCorrelationNames();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsExpressionsInOrderBy();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsOrderByUnrelated();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsGroupBy();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsGroupByUnrelated();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsGroupByBeyondSelect();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsLikeEscapeClause();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsMultipleResultSets();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsMultipleTransactions();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsNonNullableColumns();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsMinimumSQLGrammar();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsCoreSQLGrammar();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsExtendedSQLGrammar();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsANSI92EntryLevelSQL();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsANSI92IntermediateSQL();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsANSI92FullSQL();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsIntegrityEnhancementFacility();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsOuterJoins();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsFullOuterJoins();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsLimitedOuterJoins();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public String getSchemaTerm() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getSchemaTerm();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public String getProcedureTerm() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getProcedureTerm();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public String getCatalogTerm() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getCatalogTerm();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.isCatalogAtStart();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public String getCatalogSeparator() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getCatalogSeparator();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsSchemasInDataManipulation();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsSchemasInProcedureCalls();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsSchemasInTableDefinitions();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsSchemasInIndexDefinitions();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsSchemasInPrivilegeDefinitions();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsCatalogsInDataManipulation();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsCatalogsInProcedureCalls();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsCatalogsInTableDefinitions();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsCatalogsInIndexDefinitions();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsCatalogsInPrivilegeDefinitions();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsPositionedDelete();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsPositionedUpdate();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException{
        /*
         * -sf- This is not CURRENTLY supported by SpliceMachine, but when it is,
         * we will need to re-implement this.
         */
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsStoredProcedures();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsSubqueriesInComparisons();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsSubqueriesInExists();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsSubqueriesInIns();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsSubqueriesInQuantifieds();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsCorrelatedSubqueries();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsUnion() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsUnion();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsUnionAll();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsOpenCursorsAcrossCommit();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsOpenCursorsAcrossRollback();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsOpenStatementsAcrossCommit();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsOpenStatementsAcrossRollback();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxBinaryLiteralLength();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxCharLiteralLength();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxColumnNameLength();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxColumnsInGroupBy();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxColumnsInIndex();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxColumnsInOrderBy();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxColumnsInSelect();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxColumnsInTable();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxConnections() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxConnections();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxCursorNameLength();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxIndexLength() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxIndexLength();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxSchemaNameLength();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxProcedureNameLength();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxCatalogNameLength();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxRowSize() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxRowSize();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.doesMaxRowSizeIncludeBlobs();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxStatementLength() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxStatementLength();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxStatements() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxStatements();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxTableNameLength();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxTablesInSelect();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getMaxUserNameLength();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getDefaultTransactionIsolation();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsTransactions() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsTransactions();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsTransactionIsolationLevel(level);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsDataDefinitionAndDataManipulationTransactions();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsDataManipulationTransactionsOnly();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.dataDefinitionCausesTransactionCommit();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.dataDefinitionIgnoredInTransactions();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getProcedures(String catalog,String schemaPattern,String procedureNamePattern) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getProcedures(catalog,schemaPattern,procedureNamePattern);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog,String schemaPattern,String procedureNamePattern,String columnNamePattern) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getProcedureColumns(catalog,schemaPattern,procedureNamePattern,columnNamePattern);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getTables(String catalog,String schemaPattern,String tableNamePattern,String[] types) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getTables(catalog,schemaPattern,tableNamePattern,types);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getSchemas() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getSchemas();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getCatalogs();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getTableTypes() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getTableTypes();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getColumns(String catalog,String schemaPattern,String tableNamePattern,String columnNamePattern) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getColumns(catalog,schemaPattern,tableNamePattern,columnNamePattern);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog,String schema,String table,String columnNamePattern) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getColumnPrivileges(catalog,schema,table,columnNamePattern);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog,String schemaPattern,String tableNamePattern) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getTablePrivileges(catalog,schemaPattern,tableNamePattern);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog,String schema,String table,int scope,boolean nullable) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getBestRowIdentifier(catalog,schema,table,scope,nullable);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getVersionColumns(String catalog,String schema,String table) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getVersionColumns(catalog,schema,table);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog,String schema,String table) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getPrimaryKeys(catalog,schema,table);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getImportedKeys(String catalog,String schema,String table) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getImportedKeys(catalog,schema,table);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getExportedKeys(String catalog,String schema,String table) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getExportedKeys(catalog,schema,table);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog,String parentSchema,String parentTable,String foreignCatalog,String foreignSchema,String foreignTable) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getCrossReference(parentCatalog,parentSchema,parentTable,foreignCatalog,foreignSchema,foreignTable);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getTypeInfo();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getIndexInfo(String catalog,String schema,String table,boolean unique,boolean approximate) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getIndexInfo(catalog, schema, table, unique, approximate);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsResultSetType(type);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type,int concurrency) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsResultSetConcurrency(type,concurrency);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.ownUpdatesAreVisible(type);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.ownDeletesAreVisible(type);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.ownInsertsAreVisible(type);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.othersUpdatesAreVisible(type);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.othersDeletesAreVisible(type);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.othersInsertsAreVisible(type);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.updatesAreDetected(type);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.deletesAreDetected(type);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.insertsAreDetected(type);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsBatchUpdates();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getUDTs(String catalog,String schemaPattern,String typeNamePattern,int[] types) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getUDTs(catalog,schemaPattern,typeNamePattern,types);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public Connection getConnection() throws SQLException{
        return sourceConn;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException{
        return true;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException{
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException{
        return true;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException{
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog,String schemaPattern,String typeNamePattern) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getSuperTypes(catalog,schemaPattern,typeNamePattern);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getSuperTables(String catalog,String schemaPattern,String tableNamePattern) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getSuperTypes(catalog,schemaPattern,tableNamePattern);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getAttributes(String catalog,String schemaPattern,String typeNamePattern,String attributeNamePattern) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getAttributes(catalog,schemaPattern,typeNamePattern,attributeNamePattern);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.supportsResultSetHoldability(holdability);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getResultSetHoldability() throws SQLException{
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getDatabaseMajorVersion();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getDatabaseMinorVersion();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException{
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException{
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getSQLStateType();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.locatorsUpdateCopy();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException{
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException{
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog,String schemaPattern) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getSchemas(catalog,schemaPattern);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException{
        return true;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException{
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getClientInfoProperties();
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getFunctions(String catalog,String schemaPattern,String functionNamePattern) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getFunctions(catalog, schemaPattern, functionNamePattern);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog,String schemaPattern,String functionNamePattern,String columnNamePattern) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getFunctionColumns(catalog, schemaPattern, functionNamePattern,columnNamePattern);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog,String schemaPattern,String tableNamePattern,String columnNamePattern) throws SQLException{
        int numRetries = maxRetries;
        SQLException error = null;
        while(numRetries>0){
            reopenIfNecessary();
            try{
                return delegate.getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
            }catch(SQLException se){
                disconnect();
                if(error==null) error = se;
                else error.setNextException(se);
                if(!ClientErrors.isNetworkError(se)) throw error;
            }
        }
        throw error;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException{
        return true;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException{
        throw new SQLFeatureNotSupportedException("unwrap("+iface+")");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException{
        return false;
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private void reopenIfNecessary() throws SQLException{
        if(delegate!=null) return;
        conn = connectionManager.acquireConnection();
        delegate = conn.element().getMetaData();
    }

    private void disconnect(){
        delegate=null;
        try{
            conn.release();
        }catch(SQLException err){
            LOGGER.log(Level.WARNING,"unexpected error releasing connection",err);
        }
        conn = null;
        try{
            conn = connectionManager.forceReacquireConnection();
        }catch(SQLException err){
            LOGGER.log(Level.WARNING,"unexpected error acquiring connection",err);
        }
    }
}
