package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.ResultSetBuilder;
import com.splicemachine.derby.management.OperationInfo;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.derby.management.StatementManagement;
import com.splicemachine.hbase.jmx.JMXUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Strings;

import javax.management.MalformedObjectNameException;
import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;


/**
 * @author Scott Fines
 *         Date: 4/26/16
 */
public class StatementAdmin extends BaseAdminProcedures{

    public static void SYSCS_GET_STATEMENT_DETAILS(final long statementUuid,final ResultSet[] resultSets) throws SQLException{
        operate(new JMXServerOperation(){
            @Override
            public void operate(List<Pair<String, JMXConnector>> jmxConnector) throws MalformedObjectNameException, IOException, SQLException{
                List<Pair<String, StatementManagement>> statementManagers=JMXUtils.getStatementManagers(jmxConnector);
                try{
                    final ResultSetBuilder rsB=new ResultSetBuilder();
                    ResultSetBuilder.ColumnBuilder columnBuilder=rsB.getColumnBuilder();
                    columnBuilder.addColumn("Executed Plan",Types.VARCHAR);

                    for(Pair<String, StatementManagement> managementPair : statementManagers){
                        StatementManagement sm=managementPair.getSecond();
                        StatementInfo si=sm.findStatement(statementUuid);
                        if(si==null) continue;

                        OperationTreeNode otn = buildOperationTree(si);
                        ResultSetBuilder.RowBuilder rowBuilder=rsB.getRowBuilder();
                        rowBuilder.getDvd(0).setValue(otn.buildTreeString());
                        rowBuilder.addRow();
                    }
                    resultSets[0] = rsB.buildResultSet((EmbedConnection)getDefaultConn());
                }catch(StandardException se){
                    throw PublicAPI.wrapStandardException(se);
                }
            }
        });
    }

    static OperationTreeNode buildOperationTree(StatementInfo si){
        Set<OperationInfo> operationInfo=si.getOperationInfo();
        List<OperationTreeNode> subTrees =new ArrayList<>(operationInfo.size());
        for(OperationInfo oi : operationInfo){
            boolean found = false;
            for(OperationTreeNode subRoot:subTrees){
                if(subRoot.push(oi)){
                    found = true;
                    break;
                }
            }
            if(found) continue;
            OperationTreeNode newRoot = new OperationTreeNode(oi);
            for(int i=0;i<subTrees.size();i++){
                OperationTreeNode subTree = subTrees.get(i);
                if(newRoot.push(subTree)){
                    subTrees.set(i,newRoot);
                    found=true;
                    break;
                }
            }
            if(!found)
                subTrees.add(newRoot);
        }

        /*
         * We now have a list of subtrees. For each subtree then, find it's immediate parent.
         * If the parent can be found, then remove it from the list. Repeat this process until
         * only one tree remains
         */
        OperationTreeNode[] treeNodes = new OperationTreeNode[subTrees.size()];
        treeNodes = subTrees.toArray(treeNodes);
        int toMerge = treeNodes.length;
        while(toMerge>1){
            for(int i=0;i<treeNodes.length;i++){
                if(treeNodes[i]==null) continue;
                OperationTreeNode otn=treeNodes[i];
                for(int j=0;j<treeNodes.length;j++){
                    if(treeNodes[j]==null) continue;
                    OperationTreeNode subT=treeNodes[j];
                    if(subT.push(otn)){
                        treeNodes[i]=null;
                        toMerge--;
                        break;
                    }
                }
            }
        }
        for(int i=0;i<treeNodes.length;i++){
            if(treeNodes[i]!=null)
                return treeNodes[i];
        }
        throw new IllegalStateException("Programmer error: did not build the tree properly!");
    }

    /*private helper methods*/
    static class OperationTreeNode{
        private List<OperationTreeNode> children = new LinkedList<>();

        private final OperationInfo info;

        OperationTreeNode(OperationInfo info){
            this.info=info;
        }

        public boolean push(OperationInfo info){
            if(info.getParentOperationUuid()==this.info.getOperationUuid()){
                //is a child of this node
                children.add(new OperationTreeNode(info));
                return true;
            }
            for(OperationTreeNode child:children){
                if(child.push(info))
                    return true;
            }
            return false;
        }

        public boolean push(OperationTreeNode subtree){
            if(subtree.info.getParentOperationUuid()==info.getOperationUuid()){
                //is a child of this node
                children.add(subtree);
                return true;
            }
            for(OperationTreeNode child:children){
                if(child.push(subtree))
                    return true;
            }
            return false;
        }

        public String buildTreeString(){
            StringBuilder sb = new StringBuilder();
            buildString(sb,0,!checkComplete());
            return sb.toString();
        }

        private boolean checkComplete(){
            if(info.getNumJobs()>0)
                if(info.getRunningTasks()>0) return false;
            for(OperationTreeNode otn:children){
                if(!otn.checkComplete()) return false;
            }
            return true;
        }

        private void buildString(StringBuilder sb,int level,boolean running){
            for(int i=0;i<level;i++){
                sb = sb.append("  ");
            }
            if(info.getNumJobs()>0){
                running = info.getRunningTasks()>0;
            }
            sb = sb.append(infoString(running));
            sb = sb.append("\n");
            for(OperationTreeNode otn:children){
                otn.buildString(sb,level+1,running);
            }
        }

        private String infoString(boolean isRunning){
            int running = getRunningTasks(true);
            int failed = getNumFailedTasks(true);
            int total = getNumTasks(true);
            int jobs = getNumJobs(true);

            String prefix = isRunning?"-> ":"xx ";
            String val = prefix+info.getOperationTypeName()+"["+(isRunning?"RUNNING":"COMPLETE")+"]";
            if(jobs<=0) return val;
            else return val +
                    "(numJobs="+jobs+
                    ",numTasks="+total+
                    ",failedTasks="+failed+
                    ",runningTasks="+running+
                    ")";
        }

        private int getRunningTasks(boolean isTop){
            if(!isTop && info.getNumJobs()>0) return 0;
            int numRunning = info.getRunningTasks();
            for(OperationTreeNode otn:children){
                numRunning+=otn.getRunningTasks(false);
            }
            return numRunning;
        }

        private int getNumFailedTasks(boolean isTop){
            if(!isTop && info.getNumJobs()>0) return 0;
            int numFailed = info.getNumFailedTasks();
            for(OperationTreeNode otn:children){
                numFailed+=otn.getNumFailedTasks(false);
            }
            return numFailed;
        }

        private int getNumTasks(boolean isTop){
            if(!isTop && info.getNumJobs()>0) return 0;
            int numFailed = info.getNumTasks();
            for(OperationTreeNode otn:children){
                numFailed+=otn.getNumTasks(false);
            }
            return numFailed;
        }

        private int getNumJobs(boolean isTop){
            if(!isTop && info.getNumJobs()>0) return 0;
            int numFailed = info.getNumJobs();
            for(OperationTreeNode otn:children){
                numFailed+=otn.getNumJobs(false);
            }
            return numFailed;
        }
    }
}
