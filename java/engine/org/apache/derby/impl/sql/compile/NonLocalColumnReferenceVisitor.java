package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


/**
 * This Visitor finds all tables in a given tree and all columns that don't reference
 * tables found in the tree, i.e. the column may reference a table in a different tree,
 * such as the other side of the join.
 *
 */
public class NonLocalColumnReferenceVisitor implements Visitor {

    private Set tableUUIDs;
    private List colRefs;

    public NonLocalColumnReferenceVisitor(){
        tableUUIDs = new HashSet();
        colRefs = new LinkedList();
    }


    public Visitable visit(Visitable node) throws StandardException {

        if(node instanceof FromBaseTable){

            FromBaseTable tableNode = (FromBaseTable) node;
            tableUUIDs.add(tableNode.getTableDescriptor().getUUID());

        }else if(node instanceof ColumnReference){

            ColumnReference cr = (ColumnReference) node;

            if(cr.getSource() != null){
                colRefs.add(cr);
            }
        }

        return node;
    }


    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }


    public boolean stopTraversal() {
        return false;
    }


    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }

    /**
     * The visitor collects tables found in a branch of the tree, this method
     * return true if the ResultColumn's source table has been found by the Visitor.
     * It will return false if the source table has not been found (i.e. it exists in another
     * area of the plan, such as the opposite side of the join.
     *
     * @param rc
     * @return
     */
    private boolean isTableLocal(ResultColumn rc){

        boolean result = true;

        ColumnDescriptor colDescriptor = rc.getTableColumnDescriptor();

        if(colDescriptor != null){
            result = tableUUIDs.contains(rc.getTableColumnDescriptor().getReferencingUUID());
        }

        return result;
    }

    /**
     * Return a list of ColumnReference objects that point to a table not
     * found in this tree.
     *
     * @return list of ColumnReference objects
     */
    public List getNonLocalColumnRefs(){

        Iterator it = colRefs.iterator();

        while(it.hasNext()){
            ColumnReference cr = (ColumnReference) it.next();

            ResultColumn rc = cr.getSource();

            if(isTableLocal(rc)){
                it.remove();
            }

        }

        return colRefs;
    }
}
