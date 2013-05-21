package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Strings;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.RowProviders;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation.NodeType;
import com.splicemachine.utils.SpliceLogUtils;


/**
 * Takes a quantified predicate subquery's result set.
 * NOTE: A row with a single column containing null will be returned from
 * getNextRow() if the underlying subquery ResultSet is empty.
 *
 */
public class AnyOperation extends SpliceBaseOperation {
	private static Logger LOG = Logger.getLogger(AnyOperation.class);
    private static final List<NodeType> nodeTypes = Collections.singletonList(NodeType.SCAN);

	/* Used to cache row with nulls for case when subquery result set
	 * is empty.
	 */
	private ExecRow rowWithNulls;

	/* Used to cache the StatementContext */
	private StatementContext statementContext;

    // set in constructor and not altered during
    // life of object.
    public SpliceOperation source;
	private GeneratedMethod emptyRowFun;
    private String emptyRowFunName;

	public int subqueryNumber;
	public int pointOfAttachment;

    //
    // class interface
    //


    public AnyOperation() { }

    public AnyOperation(NoPutResultSet s, Activation a, GeneratedMethod emptyRowFun,
						int resultSetNumber, int subqueryNumber,
						int pointOfAttachment,
						double optimizerEstimatedRowCount,
						double optimizerEstimatedCost) throws StandardException {
		super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        source = (SpliceOperation) s;
		this.emptyRowFun = emptyRowFun;
		this.subqueryNumber = subqueryNumber;
		this.pointOfAttachment = pointOfAttachment;
        this.emptyRowFunName = emptyRowFun.getMethodName();

        init(SpliceOperationContext.newContext(a));
    }

    @Override
    public List<NodeType> getNodeTypes() {
        return nodeTypes;
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Arrays.asList(source);
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return source;
    }

    @Override
    public ExecRow getNextRowCore() throws StandardException {
        ExecRow candidateRow = source.getNextRowCore();
        ExecRow result;
        if(candidateRow!=null)
           result = candidateRow;
        else if(rowWithNulls==null){
            rowWithNulls = (ExecRow)emptyRowFun.invoke(activation);
            result = rowWithNulls;
        }else
            result = rowWithNulls;

        setCurrentRow(result);

        return result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(emptyRowFunName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        emptyRowFunName = in.readUTF();
    }

    @Override
    public void openCore() throws StandardException {
        super.openCore();
        source.openCore();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        super.init(context);
        source.init(context);

        if(emptyRowFun==null)
            emptyRowFun = context.getPreparedStatement().getActivationClass().getMethod(emptyRowFunName);
    }

    @Override
    public NoPutResultSet executeScan() throws StandardException {
        RowProvider provider = getReduceRowProvider(source,source.getExecRowDefinition());
        return new SpliceNoPutResultSet(activation,this,provider);
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);
        return new StringBuilder("Any:")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("Source:").append(source.prettyPrint(indentLevel+1))
                .append(indent).append("emptyRowFunName:").append(emptyRowFunName)
                .append(indent).append("subqueryNumber:").append(subqueryNumber)
                .toString();
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, ExecRow template) throws StandardException {
        return source.getMapRowProvider(top,template);
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, ExecRow template) throws StandardException {
        return new RowProviders.DelegatingRowProvider(source.getReduceRowProvider(top,template)) {
            @Override
            public ExecRow next() throws StandardException {
                ExecRow candidateRow = provider.next();
                ExecRow result;
                if(candidateRow!=null)
                    result = candidateRow;
                else if(rowWithNulls==null){
                    rowWithNulls = (ExecRow)emptyRowFun.invoke(activation);
                    result = rowWithNulls;
                }else
                    result = rowWithNulls;

                setCurrentRow(result);

                return result;
            }
        };
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        return source.getExecRowDefinition();
    }
}