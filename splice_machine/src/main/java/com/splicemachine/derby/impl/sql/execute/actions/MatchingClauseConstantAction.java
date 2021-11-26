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
import com.splicemachine.db.impl.sql.compile.MatchingClauseNode;
import com.splicemachine.db.impl.sql.execute.*;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.derby.impl.sql.execute.operations.CursorOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;

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
    public static final int _overflowToConglomThreshold = 10000;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // constructor args
    private int _clauseType;
    private String  _matchRefinementName;
    private ResultDescription   _thenColumnSignature;
    private String  _rowMakingMethodName;
    private int[]   _deleteColumnOffsets;
    private String  _resultSetFieldName;
    private String  _actionMethodName;
    private ConstantAction  _thenAction;

    // faulted in or built at run-time
    private transient GeneratedMethod _matchRefinementMethod;
    private transient GeneratedMethod _rowMakingMethod;
    private transient DMLWriteOperation _actionRS;

    private TemporaryRowHolderImpl _thenRows;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////
    /** 0-arg constructor needed by Formatable machinery */
    public  MatchingClauseConstantAction() {}

    /**
     * Construct from thin air.
     *
     * @param   clauseType  WHEN_NOT_MATCHED_THEN_INSERT, WHEN_MATCHED_THEN_UPDATE, WHEN_MATCHED_THEN_DELETE
     * @param   matchRefinementName Name of the method which evaluates the boolean expression in the WHEN clause.
     * @param   thenColumnSignature The shape of the row which goes into the temporary table.
     * @param   thenColumns Column positions (1-based) from the driving left join which are needed to execute the THEN clause.
     * @param   resultSetFieldName  Name of the field which will be stuffed at runtime with the temporary table of relevant rows.
     * @param   actionMethodName    Name of the method which invokes the INSERT/UPDATE/DELETE action.
     * @param   thenAction  The ConstantAction describing the associated INSERT/UPDATE/DELETE action.
     */
    public  MatchingClauseConstantAction
    (
            int clauseType,
            String matchRefinementName,
            ResultDescription thenColumnSignature,
            String rowMakingMethodName,
            int[] thenColumns,
            String resultSetFieldName,
            String actionMethodName,
            ConstantAction thenAction
    )
    {
        _clauseType = clauseType;
        _matchRefinementName = matchRefinementName;
        _thenColumnSignature = thenColumnSignature;
        _rowMakingMethodName = rowMakingMethodName;
        _deleteColumnOffsets = ArrayUtil.copy( thenColumns );
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

    public  boolean isWhenMatchedClause()
    {
        return _clauseType == MatchingClauseNode.WHEN_MATCHED_THEN_UPDATE ||
               _clauseType == MatchingClauseNode.WHEN_MATCHED_THEN_DELETE;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ConstantAction BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * executes the INSERT/UPDATE/DELETE actions with the rows that have been buffered for execution in _thenRows
     */
    public void executeConstantAction( Activation activation )
            throws StandardException
    {
        // nothing to do if no rows qualified
        if ( _thenRows == null || _thenRows.getLastArraySlot() < 0) { return; }

        CursorResultSet sourceRS = _thenRows.getResultSet();
        CursorOperation co = new CursorOperation(activation, sourceRS);

        // this is weird, but needed because result description was taken from HalfOuterJoinNode
        ResultDescription previousResultDesc = co.getActivation().getResultDescription();
        co.getActivation().setResultDescription(_thenColumnSignature);

        //
        // Push the action-specific ConstantAction rather than the Merge statement's
        // ConstantAction. The INSERT/UPDATE/DELETE expects the default ConstantAction
        // to be appropriate to it.
        //
        try {
            activation.pushConstantAction(_thenAction);

            try {
                //
                // Poke the temporary table into the variable which will be pushed as
                // an argument to the INSERT/UPDATE/DELETE action.
                //
                Field resultSetField = activation.getClass().getField(_resultSetFieldName);
                resultSetField.set(activation, co);

                //
                // Now execute the generated method which creates an InsertOperation,
                // UpdateOperation, or DeleteOperation
                Method actionMethod = activation.getClass().getMethod(_actionMethodName);
                _actionRS = (DMLWriteOperation) actionMethod.invoke(activation, null);
            } catch (Exception e) {
                throw StandardException.plainWrapException(e);
            }
            // this is where the INSERT/UPDATE/DELETE is processed
            _actionRS.open();
        } finally {
            activation.popConstantAction();
            co.getActivation().setResultDescription(previousResultDesc);
            co.close();
        }
        _thenRows.truncate();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OTHER PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Initialize this constant action, nulling out any transient state left over from
     * a previous use.
     * </p>
     */
    public void init() throws StandardException
    {
        _actionRS = null;
    }

    /**
     * <p>
     * Run the matching refinement clause associated with this WHEN [ NOT ] MATCHED clause.
     * The refinement is a boolean expression. Return the boolean value it resolves to.
     * A boolean NULL is treated as false. If there is no refinement clause, then this method
     * evaluates to true.
     * </p>
     */
    public boolean satisfiesMatchingRefinement(Activation activation)
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
    public void bufferThenRow(Activation activation, ExecRow selectRow) throws StandardException
    {
        if ( _thenRows == null ) { _thenRows = createThenRows( activation ); }

        ExecRow thenRow;
        if(_clauseType == MatchingClauseNode.WHEN_MATCHED_THEN_DELETE) {
            thenRow = copyRowWithOffsets(selectRow, _deleteColumnOffsets);
        }
        else {
            thenRow = bufferThenRowInternal( activation, selectRow );
        }

        _thenRows.insert( thenRow );
        // prevent TemporaryRowHolderImpl from creating a conglomerate,
        // which is currently not supported as it runs in read-only transaction
        // maybe it's faster anyway to do it like this and not write to hbase
        if(_thenRows.numRows() >= _overflowToConglomThreshold ) {
            executeConstantAction(activation);
        }
    }

    /**
     * <p>
     * Construct a ValueRow by creating a copy with column offsets.
     * This is used when buffering a row for DELETE
     * The passed-in row is the SELECT list of the MERGE statement's driving left join.
     * </p>
     */
    public static ExecRow copyRowWithOffsets(ExecRow selectRow, int[] offsets) throws StandardException
    {
        int rowLength = offsets.length;
        ValueRow thenRow = new ValueRow( rowLength );
        for ( int i = 0; i < rowLength; i++ ) {
            thenRow.setColumn( i + 1, selectRow.getColumn( offsets[ i ] ) );
        }
        return thenRow;
    }

    /**
     * <p>
     * Construct and buffer a row for the INSERT/UPDATE
     * action corresponding to this MATCHED clause.
     * </p>
     */
    ExecRow bufferThenRowInternal(Activation activation, ExecRow row)
            throws StandardException
    {
        if ( _rowMakingMethod == null ) {
            _rowMakingMethod = ((BaseActivation) activation).getMethod( _rowMakingMethodName );
        }

        // we get the row in the generated code from the activation
        return (ExecRow) _rowMakingMethod.invoke( activation );
    }

    /**
     * <p>
     * Release resources at the end.
     * </p>
     */
    public void cleanUp() throws StandardException
    {
        if ( _actionRS != null )
        {
            _actionRS.close();
            _actionRS = null;
        }

        _matchRefinementMethod = null;
        _rowMakingMethod = null;

        if ( _thenRows != null )
        {
            _thenRows.close();
            _thenRows = null;
        }

    }


    /**
     * <p>
     * Create the temporary table for holding the rows which are buffered up
     * for bulk-processing after the driving left join completes.
     * </p>
     */
    private TemporaryRowHolderImpl createThenRows( Activation activation )
    {
        return new TemporaryRowHolderImpl( activation, new Properties(), _thenColumnSignature, _overflowToConglomThreshold );
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
        // int oldVersion =
                in.readInt();

        _clauseType = in.readInt();
        _matchRefinementName = (String) in.readObject();
        _thenColumnSignature = (ResultDescription) in.readObject();
        _rowMakingMethodName = (String) in.readObject();
        _deleteColumnOffsets = ArrayUtil.readIntArray( in );
        _resultSetFieldName = (String) in.readObject();
        _actionMethodName = (String) in.readObject();
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
        out.writeObject( _matchRefinementName );
        out.writeObject( _thenColumnSignature );
        out.writeObject( _rowMakingMethodName );
        ArrayUtil.writeIntArray( out, _deleteColumnOffsets );
        out.writeObject( _resultSetFieldName );
        out.writeObject( _actionMethodName );
        out.writeObject( _thenAction );
    }

    /**
     * Get the formatID which corresponds to this class.
     *
     *	@return	the formatID of this class
     */
    public int getTypeFormatId() { return StoredFormatIds.MATCHING_CLAUSE_CONSTANT_ACTION_V01_ID; }

    /**
     * @param activation  the activation to use
     * @param row         the row from the driving left join
     * @param matched     true if this is a "matching" row (has RowLocation), false if it is a "not matched" row (no RowLocation)
     * @return true if row has been consumed by this matching clause, otherwise false
     */
    public boolean consumeRow(Activation activation, ExecRow row, boolean matched) throws StandardException {

        // if we have a match, consider a clause if it is WHEN MATCHED THEN xxx
        // else (we have a non-match), consider a clause if it's not a WHEN MATCHED (e.g. WHEN NOT MATCHED THEN INSERT)
        if(matched != isWhenMatchedClause())
            return false;

        // check if matching refinement is satisfied
        if ( satisfiesMatchingRefinement( activation ) ) {
            // buffer the row, execute if buffer exceeded (@sa MatchingClauseConstantAction._overflowToConglomThreshold)
            bufferThenRow( activation, row );
            return true;
        }
        else {
            return false;
        }
    }
}
