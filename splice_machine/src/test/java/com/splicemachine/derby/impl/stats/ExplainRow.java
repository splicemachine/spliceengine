/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.stats;

import org.junit.Assert;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Scott Fines
 *         Date: 4/1/15
 */
public class ExplainRow{

    /*
     * This is unfortunately fragile--if the format of the Explain string changes, then
     * this logic will likely break. Still, until we support something like GSON formatting in
     * EXPLAIN logic, this is the best that we can do
     */
    private static Pattern costPattern=Pattern.compile("cost=\\(.*?\\)");
    private static Pattern rsnPattern=Pattern.compile("n=[0-9]+");
    private static Pattern tablePattern=Pattern.compile("Scan\\[.*\\([0-9]+\\)\\]");
    private static Pattern indexPattern=Pattern.compile("using-index=.*\\([0-9]+\\)");
    private static Pattern qualsPattern=Pattern.compile("quals=.*\\[.*?\\]");


    public enum Type{
        TABLESCAN("TableScan"),
        PROJECTRESTRICT("ProjectRestrictNode"),
        INDEXLOOKUP("IndexLookup"),
        ORDERBY("OrderBy"),
        GROUPBY("GroupBy"),
        JOIN("Join");

        private final String formatString;

        public static Type forLabel(String label){
            for(Type type : values()){
                if(label.startsWith(type.formatString)) return type;
            }
            throw new IllegalArgumentException("Cannot parse type for label "+label);
        }

        Type(String formatString){
            this.formatString=formatString;
        }
    }
    private final Type type;
    private final Cost cost;
    private final int rsn;
    private final String tableName;
    private final String indexName;
    private final String quals;

    public static ExplainRow parse(String row){
        //example row
        //FromBaseTable ({cost=(overallCost=11,localCost=10,remoteCost=1,outputRows=10,outputHeapSize=1020,partitions=1), n=0, table=SYSTABLES(32)})
//        FromBaseTable ({cost=(overallCost=466.53,localCost=411,remoteCost=55.53,outputRows=117,outputHeapSize=117,partitions=1), quals=[(B[0:1] < 10)], n=0, table=DT(1360)})
        String label=row.substring(0,row.indexOf("(")).trim();
        Type type=Type.forLabel(label);

//        Matcher matcher=costPattern.matcher(row);
//        Assert.assertTrue("No Cost value returned!",matcher.find());
        Cost cost=Cost.parse(row);

        Matcher matcher=rsnPattern.matcher(row);
        Assert.assertTrue("No Result Set Number returned!",matcher.find());
        int rsn=parseRsn(matcher.group());

        String tableName=null;
        String indexName=null;
        if(type==Type.TABLESCAN){
            matcher=tablePattern.matcher(row);
            Assert.assertTrue("No Table name found for TableScan!",matcher.find());
            tableName=matcher.group().substring("table=".length());

            matcher=indexPattern.matcher(row);
            if(matcher.find()){
                indexName=matcher.group().substring("using-index=".length());
            }
        }
        String quals = null;
        matcher = qualsPattern.matcher(row);
        if(matcher.find()){
            quals = matcher.group().substring("quals=".length());
        }

        return new ExplainRow(type,cost,rsn,tableName,indexName,quals);
    }

    public ExplainRow(Type type,Cost cost,int rsn,String tableName,String indexName,String quals){
        this.type=type;
        this.cost=cost;
        this.rsn=rsn;
        this.tableName=tableName;
        this.indexName=indexName;
        this.quals = quals;
    }

    public Type type(){ return type; }
    public Cost cost(){ return cost; }
    public int rsn(){ return rsn; }
    public String tableName(){ return tableName; }
    public String indexName(){ return indexName; }
    public String qualifiers(){ return quals;}

    private static int parseRsn(String rsnGroup){
        int numIndex=rsnGroup.indexOf("=");
        return Integer.parseInt(rsnGroup.substring(numIndex+1));
    }

    public static class Cost{
        private static Pattern overallCostP=Pattern.compile("totalCost=[0-9]+\\.?[0-9]*");
        private static Pattern localCostP=Pattern.compile("processingCost=[0-9]+\\.?[0-9]*");
        private static Pattern remoteCostP=Pattern.compile("transferCost=[0-9]+\\.?[0-9]*");
        private static Pattern outputRowsP=Pattern.compile("outputRows=[0-9]+");
        private static Pattern outputHeapP=Pattern.compile("outputHeapSize=[0-9]+");
        private static Pattern partitionsP=Pattern.compile("partitions=[0-9]+");

        private double overallCost;
        private double localCost;
        private double remoteCost;
        private long rowCount;
        private long heapSize;
        private int partitionCount;

        public static Cost parse(String cost){
            Matcher m = overallCostP.matcher(cost);
            Assert.assertTrue("No Overall cost found!",m.find());
            double oc = Double.parseDouble(m.group().substring("totalCost=".length()));

            m = localCostP.matcher(cost);
            Assert.assertTrue("No Local cost found!",m.find());
            double lc = Double.parseDouble(m.group().substring("processingCost=".length()));

            m = remoteCostP.matcher(cost);
            Assert.assertTrue("No Remote cost found!",m.find());
            double rc = Double.parseDouble(m.group().substring("transferCost=".length()));

            m = outputRowsP.matcher(cost);
            Assert.assertTrue("No Output Rows found!",m.find());
            long or = Long.parseLong(m.group().substring("outputRows=".length()));

            m = outputHeapP.matcher(cost);
            Assert.assertTrue("No Output HeapSize found!",m.find());
            long oh = Long.parseLong(m.group().substring("outputHeapSize=".length()));

            m = partitionsP.matcher(cost);
            Assert.assertTrue("No Partition Count found!",m.find());
            int pc = Integer.parseInt(m.group().substring("partitions=".length()));

            return new Cost(oc,lc,rc,or,oh,pc);
        }

        public Cost(double overallCost,double localCost,double remoteCost,long rowCount,long heapSize,int partitionCount){
            this.overallCost=overallCost;
            this.localCost=localCost;
            this.remoteCost=remoteCost;
            this.rowCount=rowCount;
            this.heapSize=heapSize;
            this.partitionCount=partitionCount;
        }

        public double overallCost(){ return overallCost; }
        public double localCost(){ return localCost; }
        public double remoteCost(){ return remoteCost; }
        public long rowCount(){ return rowCount; }
        public long heapSize(){ return heapSize; }
        public int partitionCount(){ return partitionCount; }
    }

}
