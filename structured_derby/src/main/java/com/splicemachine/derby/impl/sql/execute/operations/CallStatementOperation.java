package com.splicemachine.derby.impl.sql.execute.operations;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.ConnectionContext;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;

class CallStatementOperation extends NoRowsResultSetOperation {
	private final GeneratedMethod methodCall;

	CallStatementOperation(GeneratedMethod methodCall,Activation a) {
		super(a);
		this.methodCall = methodCall;
	}

	/**
     * Just invoke the method.
		@exception StandardException Standard Derby error policy
	*/
	public void open() throws StandardException {
		setup();
        methodCall.invoke(activation);
    }

    /**
     * Need to explicitly close any dynamic result sets.
     * <BR>
     * If the dynamic results are not accessible then they
     * need to be destroyed (ie. closed) according the the
     * SQL Standard.
     * <BR>
     * An execution of a CALL statement through JDBC makes the
     * dynamic results accessible, in this case the closing
     * of the dynamic result sets is handled by the JDBC
     * statement object (EmbedStatement) that executed the CALL.
     * We cannot unify the closing of dynamic result sets to
     * this close, as in accessible case it is called during
     * the Statement.execute call, thus it would close the
     * dynamic results before the application has a change
     * to use them.
     * 
     * <BR>
     * With an execution of a CALL
     * statement as a trigger's action statement the dynamic
     * result sets are not accessible. In this case this close
     * method is called after the execution of the trigger's
     * action statement.
     * <BR>
     * <BR>
     * Section 4.27.5 of the TECHNICAL CORRIGENDUM 1 to the SQL 2003
     * Standard details what happens to dynamic result sets in detail,
     * the SQL 2003 foundation document is missing these details.
     */
    public void close() throws StandardException {
        super.close();
        ResultSet[][] dynamicResults = getActivation().getDynamicResults();
        if (dynamicResults != null) {
            // Need to ensure all the result sets opened by this
            // CALL statement for this connection are closed.
            // If any close() results in an exception we need to keep going,
            // save any exceptions and then throw them once we are complete.
            StandardException errorOnClose = null;
            
            ConnectionContext jdbcContext = null;
            
            for (int i = 0; i < dynamicResults.length; i++)
            {
                ResultSet[] param = dynamicResults[i];
                ResultSet drs = param[0];
                
                // Can be null if the procedure never set this parameter
                // or if the dynamic results were processed by JDBC (EmbedStatement).
                if (drs == null)
                    continue;
                
                if (jdbcContext == null)
                    jdbcContext = (ConnectionContext)
                   lcc.getContextManager().getContext(ConnectionContext.CONTEXT_ID);
               
                try {
                    
                    // Is this a valid, open dynamic result set for this connection?
                    if (!jdbcContext.processInaccessibleDynamicResult(drs))
                    {
                        // If not just ignore it, not Derby's problem.
                        continue;
                    }
                    
                    drs.close();
                    
                } catch (SQLException e) {
                    
                    // Just report the first error
                    if (errorOnClose == null)
                    {
                        StandardException se = StandardException.plainWrapException(e);
                        errorOnClose = se;
                    }
                }
                finally {
                    // Remove any reference to the ResultSet to allow
                    // it and any associated resources to be garbage collected.
                    param[0] = null;
                }
            }
            
            if (errorOnClose != null)
                throw errorOnClose;
        }       
    }

	/**
	 * @see org.apache.derby.iapi.sql.ResultSet#cleanUp
	 */
	public void	cleanUp() throws StandardException
	{
			close();
	}
}

