package com.splicemachine.derby.impl.sql.execute.actions;


import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.db.impl.sql.execute.TemporaryRowHolderImpl;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.impl.sql.execute.ValueRow;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * Describes the execution machinery needed to evaluate a WHEN [ NOT ] MATCHING clause
 * of a MERGE statement.
 */
public class MatchingClauseConstantAction implements ConstantAction, Formatable
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Serial version produced by the serialver utility. Needed in order to
     * make serialization work reliably across different compilers.
     */
    private static  final   long    serialVersionUID = -6725483265211088817L;

    // for versioning during (de)serialization
    private static final int FIRST_VERSION = 0;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // constructor args
    private int _clauseType;
    private String  _matchRefinementName;
    private int[]   _thenColumnOffsets;
    private String  _resultSetFieldName;
    private String  _actionMethodName;
    private ConstantAction  _thenAction;

    // faulted in or built at run-time
    private transient GeneratedMethod _matchRefinementMethod;
    private transient TemporaryRowHolderImpl _thenRows;
    private transient ResultSet _actionRS;

    public  MatchingClauseConstantAction() {}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Construct from thin air.
     *
     * @param   clauseType  WHEN_NOT_MATCHED_THEN_INSERT, WHEN_MATCHED_THEN_UPDATE, WHEN_MATCHED_THEN_DELETE
     * @param   matchRefinementName Name of the method which evaluates the boolean expression in the WHEN clause.
     * @param   thenColumns Column positions (1-based) from the driving left join which are needed to execute the THEN clause.
     * @param   resultSetFieldName  Name of the field which will be stuffed at runtime with the temporary table of relevant rows.
     * @param   actionMethodName    Name of the method which invokes the INSERT/UPDATE/DELETE action.
     * @param   thenAction  The ConstantAction describing the associated INSERT/UPDATE/DELETE action.
     */
    public  MatchingClauseConstantAction
    (
            int    clauseType,
            String matchRefinementName,
            int[]  thenColumns,
            String resultSetFieldName,
            String actionMethodName,
            ConstantAction thenAction
    )
    {
        _clauseType = clauseType;
        _matchRefinementName = matchRefinementName;
        _thenColumnOffsets = ArrayUtil.copy( thenColumns );
        _resultSetFieldName = resultSetFieldName;
        _actionMethodName = actionMethodName;
        _thenAction = thenAction;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ACCESSORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Get the clause type: WHEN_NOT_MATCHED_THEN_INSERT, WHEN_MATCHED_THEN_UPDATE, WHEN_MATCHED_THEN_DELETE */
    public  int clauseType() { return _clauseType; }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ConstantAction BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public void	executeConstantAction( Activation activation )
            throws StandardException
    {
        // nothing to do if no rows qualified
        if ( _thenRows == null ) { return; }

        CursorResultSet sourceRS = _thenRows.getResultSet();
        GeneratedMethod actionGM = ((BaseActivation) activation).getMethod( _actionMethodName );

        //
        // Push the action-specific ConstantAction rather than the Merge statement's
        // ConstantAction. The INSERT/UPDATE/DELETE expects the default ConstantAction
        // to be appropriate to it.
        //
        try {
            activation.pushConstantAction( _thenAction );

            try {
                //
                // Poke the temporary table into the variable which will be pushed as
                // an argument to the INSERT/UPDATE/DELETE action.
                //
                Field resultSetField = activation.getClass().getField( _resultSetFieldName );
                resultSetField.set( activation, sourceRS );

                //
                // Now execute the generated method which creates an InsertResultSet,
                // UpdateResultSet, or DeleteResultSet.
                //
                Method actionMethod = activation.getClass().getMethod( _actionMethodName );
                _actionRS = (ResultSet) actionMethod.invoke( activation, null );
            }
            catch (Exception e) { throw StandardException.plainWrapException( e ); }

            // this is where the INSERT/UPDATE/DELETE is processed
            _actionRS.open();
        }
        finally
        {
            activation.popConstantAction();
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OTHER PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Run the matching refinement clause associated with this WHEN [ NOT ] MATCHED clause.
     * The refinement is a boolean expression. Return the boolean value it resolves to.
     * A boolean NULL is treated as false. If there is no refinement clause, then this method
     * evaluates to true.
     * </p>
     */
    public boolean evaluateRefinementClause(Activation activation)
            throws StandardException
    {
        if ( _matchRefinementName == null ) { return true; }
        if ( _matchRefinementMethod == null )
        {
            _matchRefinementMethod = ((BaseActivation) activation).getMethod( _matchRefinementName );
        }

        SQLBoolean result = (SQLBoolean) _matchRefinementMethod.invoke( activation );

        if ( result.isNull() ) { return false; }
        else { return result.getBoolean(); }
    }

    /**
     * <p>
     * Construct and buffer a row for the INSERT/UPDATE/DELETE
     * action corresponding to this [ NOT ] MATCHED clause. The buffered row
     * is built from columns in the passed-in row. The passed-in row is the SELECT list
     * of the MERGE statement's driving left join.
     * </p>
     */
    public void    bufferThenRow
    (
            Activation activation,
            ResultDescription selectDescription,
            ExecRow selectRow
    ) throws StandardException
    {
        int             thenRowLength = _thenColumnOffsets.length;
        ValueRow thenRow = new ValueRow( thenRowLength );
        for ( int i = 0; i < thenRowLength; i++ )
        {
            thenRow.setColumn( i + 1, selectRow.getColumn( _thenColumnOffsets[ i ] ) );
        }

        if ( _thenRows == null ) { createThenRows( activation, selectDescription ); }
        _thenRows.insert( thenRow );
    }

    /**
     * <p>
     * Release resources at the end.
     * </p>
     */
    public void    cleanUp()   throws StandardException
    {
        if ( _actionRS != null ) { _actionRS.close(); }
        if ( _thenRows != null ) { _thenRows.close(); }
    }

    /**
     * <p>
     * Create the temporary table for holding the rows which are buffered up
     * for bulk-processing after the driving left join completes.
     * </p>
     */
    private void    createThenRows( Activation activation, ResultDescription selectDescription )
            throws StandardException
    {
        ResultDescription   thenDescription = activation.getLanguageConnectionContext().getLanguageFactory().getResultDescription
                ( selectDescription, _thenColumnOffsets );

        _thenRows = new TemporaryRowHolderImpl( activation, new Properties(), thenDescription );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Formatable BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Read this object from a stream of stored objects.
     *
     * @param in read this.
     *
     * @exception IOException					thrown on error
     * @exception ClassNotFoundException		thrown on error
     */
    public void readExternal( ObjectInput in )
            throws IOException, ClassNotFoundException
    {
        // as the persistent form evolves, switch on this value
        int oldVersion = in.readInt();

        _clauseType = in.readInt();
        _matchRefinementName = in.readUTF();
        _thenColumnOffsets = ArrayUtil.readIntArray( in );
        _resultSetFieldName = in.readUTF();
        _actionMethodName = in.readUTF();
        _thenAction = (ConstantAction) in.readObject();
    }

    /**
     * Write this object to a stream of stored objects.
     *
     * @param out write bytes here.
     *
     * @exception IOException		thrown on error
     */
    public void writeExternal( ObjectOutput out )
            throws IOException
    {
        out.writeInt( FIRST_VERSION );

        out.writeInt( _clauseType );
        out.writeUTF( _matchRefinementName );
        ArrayUtil.writeIntArray( out, _thenColumnOffsets );
        out.writeUTF( _resultSetFieldName );
        out.writeUTF( _actionMethodName );
        out.writeObject( _thenAction );
    }

    /**
     * Get the formatID which corresponds to this class.
     *
     *	@return	the formatID of this class
     */
    public	int	getTypeFormatId()	{ return StoredFormatIds.MATCHING_CLAUSE_CONSTANT_ACTION_V01_ID; }

}
