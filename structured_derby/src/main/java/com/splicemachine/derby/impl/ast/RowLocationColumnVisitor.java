package com.splicemachine.derby.impl.ast;

import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.*;
import org.apache.derby.iapi.util.ReuseFactory;
import org.apache.derby.impl.sql.compile.*;
import org.apache.log4j.Logger;

import java.util.*;

public class RowLocationColumnVisitor extends AbstractSpliceVisitor {

    private static Logger LOG = Logger.getLogger(RowLocationColumnVisitor.class);

    @Override
    public Visitable visit(DeleteNode node) throws StandardException {
        return doVisit(node);
    }

    @Override
    public Visitable visit(UpdateNode node) throws StandardException {
        return doVisit(node);
    }
    
    protected Visitable doVisit(DMLStatementNode node) throws StandardException {

    	// We only need to deal with update and delete. Otherwise it's a no-op.
    	if (!(node instanceof UpdateNode || node instanceof DeleteNode)) {
    		return node;
    	}

    	String rowLocationResultColName = (node instanceof UpdateNode) ?
    	    UpdateNode.COLUMNNAME : DeleteNode.COLUMNNAME;
    	
    	ResultSetNode rsnRoot = node.getResultSetNode();

    	// Only continue if the operation is over a sink (e.g. update over merge join)
    	if (!(RSUtils.hasSinkingChildren(rsnRoot))) {
    		if (LOG.isTraceEnabled()) {
    			SpliceLogUtils.trace(LOG, "No sinking children found so this visitor is a no-op.");
    		}
    		return node;
    	}
    	
    	//
    	// Step 1: identify the topmost Project Restrict Node
    	//
    	
    	// Fetch the closest PRN child
    	List<ProjectRestrictNode> prnAll = RSUtils.collectNodes(rsnRoot, ProjectRestrictNode.class);
    	if (prnAll == null || prnAll.size() == 0) error("Unable to fetch descendent ProjectRestrictNodes for node %s", node);
    	ProjectRestrictNode prnUpper = (ProjectRestrictNode)prnAll.get(0);
    	if (prnUpper == null) error("Unable to fetch upper ProjectRestrictNode for node %s", node);

    	// Stash aside the row location result column from this upper PRN
    	ResultColumnList prnUpperRCL = prnUpper.getResultColumns();
    	assert prnUpperRCL != null;
    	ResultColumn prnUpperRowLocCol = prnUpperRCL.getResultColumn(rowLocationResultColName);
    	if (prnUpperRowLocCol == null) error("Unable to find the row location ResultColumn in upper ProjectRestrictNode for node %s", node);
    	
		//
		// Step 2: find leftmost path from topmost PRN to leaf
		//
		
		ResultSetNode currentNode = prnUpper;
		List<ResultSetNode> children = null;
		List<ResultSetNode> pathToLeaf = new ArrayList<ResultSetNode>();
		while (true) {
			children = RSUtils.getChildren(currentNode); // only returns RSN children
			int size = children.size();
			if (size > 1) {
				// We assume this is a binary node like a JoinNode
				if (!(RSUtils.binaryRSNs.contains(currentNode.getClass()))) {
					error("Unexpectedly found node with %s children but of non binary type %s", size, currentNode.getClass());
				}
				pathToLeaf.add(currentNode);
				currentNode = getLeftChildNode(currentNode);
			} else if (size == 1) {
				pathToLeaf.add(currentNode);
				currentNode = (ResultSetNode)children.get(0);
			} else {
				// Assume leaf node, so we are done
				if (!(RSUtils.leafRSNs.contains(currentNode.getClass()))) {
					error("Leaf node had unexpected type %s", currentNode.getClass());
				}
				break;
			}
		}

		SpliceLogUtils.debug(LOG, "Found left side leaf node %s", currentNode);
		
		if (pathToLeaf.size() < 1) {
			error("Unexpectedly found pathToLeaf list was empty when visiting node %s", node);
		}
		
		//
		// Step 3: prepare each node in the 'left path' (from step 2) to handle row location properly
		//
		
		int maxIndex = pathToLeaf.size() - 1;
		ResultSetNode pathNode = null;
		for (int i = maxIndex; i > -1; i--) {
			pathNode = (ResultSetNode)pathToLeaf.get(i);
			// TODO: handle TableOperatorNode types other than JoinNode?
			if (pathNode instanceof JoinNode) {
				// Special case for Join Node until we generalize rebuildRCL
				// to other node classes, perhaps SetOperatorNode which also
				// has buildRCL.
				((JoinNode)pathNode).rebuildRCL();
			} else if (i == 0) { // i == 0 means we are at the root (uppermost) PRN node
				ResultColumnList rcl = pathToLeaf.get(i + 1).getResultColumns();
				assert rcl != null;
				ResultColumn rcRowLoc = rcl.getResultColumn(rowLocationResultColName);
				assert rcRowLoc != null;
				
				prnUpperRowLocCol.setExpression(
					(ValueNode)prnUpper.getNodeFactory().getNode(
						C_NodeTypes.VIRTUAL_COLUMN_NODE,
						(ResultSetNode)pathToLeaf.get(i + 1), // source result set: my child node
						rcRowLoc, // source result column
						ReuseFactory.getInteger(prnUpperRowLocCol.getVirtualColumnId()), // getNode expects 1-based index and virtualColumnId provides that
						prnUpper.getContextManager())
				);
			} else {
				// We are below the root PRN, either another PRN (such as the one above the base table) or a different node in the path
				ResultColumn rcCopy = prnUpperRowLocCol.cloneMe();
				rcCopy.setResultSetNumber(pathNode.getResultSetNumber());
				pathNode.getResultColumns().addResultColumn(rcCopy);		
			}
		}
		
    	return node;
    }
    
    private void error(String msg, Object... args) {
        throw new RuntimeException(String.format(msg, args));
    }

    private ResultSetNode getLeftChildNode(ResultSetNode node) {
    	assert node instanceof TableOperatorNode;
    	return ((TableOperatorNode)node).getLeftResultSet();
    }
 }
