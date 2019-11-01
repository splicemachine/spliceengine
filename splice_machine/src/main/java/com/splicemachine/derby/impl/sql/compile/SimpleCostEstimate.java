/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicateList;
import com.splicemachine.db.iapi.sql.compile.RowOrdering;
import com.splicemachine.db.impl.sql.compile.PredicateList;

import java.text.DecimalFormat;

/**
 * @author Scott Fines
 *         Date: 3/13/15
 */
public class SimpleCostEstimate implements CostEstimate{
    private static final String[] displayHeapUnits = {" B"," KB"," MB"," GB"," TB"};
    /**
     * Note: if you add a field to this class, make sure you also make changes to cloneMe() and setCost()
     */
    private boolean isOuterJoin;
    private boolean isAntiJoin;
    private double localCost = Double.MAX_VALUE;
    private double remoteCost;
    private int numPartitions;
    private double numRows = Double.MAX_VALUE;
    private double singleScanRowCount = Double.MAX_VALUE;
    private RowOrdering rowOrdering;
    private OptimizablePredicateList predicateList;
    private SimpleCostEstimate baseCost;
    private boolean isRealCost=true;
    private long estimatedHeapSize;
    private double openCost;
    private double closeCost;
    protected double fromBaseTableCost =-1.0d;
    protected double fromBaseTableRows =-1.0d;
    protected double indexLookupCost =-1.0d;
    protected double indexLookupRows =-1.0d;
    protected double projectionCost =-1.0d;
    protected double projectionRows =-1.0d;
    protected double scannedBaseTableRows = -1.0d;
    private double localCostPerPartition;
    private double remoteCostPerPartition;
    /* consecutive broadcast joins memory used in bytes */
    private double accumulatedMemory = 0.0d;
    private boolean singleRow = false;

    public SimpleCostEstimate(){ }

    public SimpleCostEstimate(double theCost,double theRowCount,double theSingleScanRowCount){
        this(theCost,0d,theRowCount,theSingleScanRowCount,1);
    }

    public SimpleCostEstimate(double localCost,double remoteCost,double numRows,double singleScanRowCount,int numPartitions){
        this.localCost=localCost;
        this.remoteCost=remoteCost;
        this.numRows=numRows>1?numRows:1;
        this.singleScanRowCount=singleScanRowCount;
        this.numPartitions=numPartitions;
        setLocalCostPerPartition(localCost, numPartitions);
        setLocalCostPerPartition(remoteCost, numPartitions);
    }

    @Override public double getOpenCost(){ return openCost; }
    @Override public double getCloseCost(){ return closeCost; }

    @Override public void setOpenCost(double openCost){ this.openCost = openCost;}
    @Override public void setCloseCost(double closeCost){ this.closeCost = closeCost;}

    @Override
    public void setCost(double cost,double rowCount,double singleScanRowCount){
        setCost(cost,rowCount,singleScanRowCount,1);
    }

    @Override
    public void setCost(double cost,double rowCount,double singleScanRowCount,int numPartitions){
        this.localCost = cost;
        this.numRows = rowCount > 1 ? rowCount : 1;
        this.singleScanRowCount = singleScanRowCount;
        this.numPartitions = numPartitions;
        assert (numPartitions >= 1);
        this.localCostPerPartition = localCost/numPartitions;
    }

    @Override
    public void setCost(CostEstimate other){
        this.isOuterJoin = other.isOuterJoin();
        this.isAntiJoin = other.isAntiJoin();
        this.localCost = other.localCost();
        this.remoteCost = other.remoteCost();
        this.numPartitions =other.partitionCount();
        this.numRows = other.rowCount();
        this.singleScanRowCount = other.singleScanRowCount();
        RowOrdering rowOrdering=other.getRowOrdering();
        if(rowOrdering!=null)
            this.rowOrdering = rowOrdering.getClone();
        else
            this.rowOrdering = null;
        setPredicateList(other.getPredicateList());
        CostEstimate base=other.getBase();
        if(base!=null && base != other)
            this.baseCost = (SimpleCostEstimate) base.cloneMe();
        else
            this.baseCost = null;
        this.isRealCost = other.isRealCost();
        this.estimatedHeapSize = other.getEstimatedHeapSize();
        this.openCost = other.getOpenCost();
        this.closeCost = other.getCloseCost();
        this.fromBaseTableCost =  other.getFromBaseTableCost();
        this.fromBaseTableRows = other.getFromBaseTableRows();
        this.indexLookupCost = other.getIndexLookupCost();
        this.indexLookupRows = other.getIndexLookupRows();
        this.projectionCost = other.getProjectionCost();
        this.projectionRows = other.getProjectionRows();
        this.scannedBaseTableRows = other.getScannedBaseTableRows();
        this.localCostPerPartition = other.getLocalCostPerPartition();
        this.remoteCostPerPartition = other.getRemoteCostPerPartition();
        this.accumulatedMemory = other.getAccumulatedMemory();
        this.setSingleRow(other.isSingleRow());
    }

    @Override
    public String prettyProcessingString() {
        return prettyProcessingString(",");
    }

    @Override
    public String prettyProcessingString(String attrDelim) {
        return prettyStringOutput(localCost, getEstimatedRowCount(), -1, attrDelim);
    }

    @Override
    public String prettyFromBaseTableString() {
        return prettyStringOutput(getFromBaseTableCost(),Math.round(getFromBaseTableRows()), Math.round(getScannedBaseTableRows()));
    }

    @Override
    public String prettyFromBaseTableString(String attrDelim) {
        return prettyStringOutput(getFromBaseTableCost(),Math.round(getFromBaseTableRows()),Math.round(getScannedBaseTableRows()), attrDelim);
    }

    @Override
    public String prettyIndexLookupString() {
        return prettyIndexLookupString(",");
    }

    @Override
    public String prettyIndexLookupString(String attrDelim) {
        return prettyStringOutput(getIndexLookupCost(), Math.round(getIndexLookupRows()), -1, attrDelim);
    }

    @Override
    public String prettyDmlStmtString(String rowsLabel) {
        return prettyDmlStmtString(getEstimatedCost(), getEstimatedRowCount(), ",", rowsLabel);
    }

    @Override
    public String prettyDmlStmtString(double cost, long rows, String attrDelim, String rowsLabel) {
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(3);
        df.setGroupingUsed(false);
        StringBuilder sb = new StringBuilder();
        sb.append("totalCost=").append(df.format(cost/1000));
        sb.append(attrDelim).append(rowsLabel == null ? "outputRows" : rowsLabel).append("=").append(rows);
        return sb.toString();
    }

    private String prettyStringOutput(double cost, long rows, long scannedRows) {
        return prettyStringOutput(cost, rows, scannedRows, ",");
    }

    private String prettyStringOutput(double cost, long rows, long scannedRows, String attrDelim) {
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(3);
        df.setGroupingUsed(false);
        long estHeap = getEstimatedHeapSize();
        double eHeap = estHeap;
        int pos = 0;
        while(pos<displayHeapUnits.length-1 && estHeap>1024){
            eHeap/=1024;
            estHeap = (long)eHeap;
            pos++;
        }
        String unit = displayHeapUnits[pos];

        StringBuilder sb = new StringBuilder();
        sb.append("totalCost=").append(df.format(cost/1000));
        if (scannedRows != -1)
            sb.append(attrDelim).append("scannedRows=").append(scannedRows);
        sb.append(attrDelim).append("outputRows=").append(rows);
        sb.append(attrDelim).append("outputHeapSize=").append(df.format(eHeap)).append(unit);
        sb.append(attrDelim).append("partitions=").append(partitionCount());

        return sb.toString();
    }

    @Override
    public String prettyProjectionString() {
        return prettyProjectionString(",");
    }

    @Override
    public String prettyProjectionString(String attrDelim) {
        return prettyStringOutput(getProjectionCost(), Math.round(getProjectionRows()), -1, attrDelim);
    }

    @Override
    public String prettyScrollInsensitiveString() {
        return prettyScrollInsensitiveString(",");
    }

    @Override
    public String prettyScrollInsensitiveString(String attrDelim) {
        return prettyStringOutput(getEstimatedCost(), getEstimatedRowCount(), -1, attrDelim);
    }

    @Override
    public String toString(){
        return "(totalCost="+getEstimatedCost()
                +",processingCost="+localCost()
                +",transferCost="+remoteCost()
                +",outputRows="+getEstimatedRowCount()
                +",outputHeapSize="+getEstimatedHeapSize()+
                ",partitions="+partitionCount()+
                ",rowOrdering="+rowOrdering+
                ",predicateList="+predicateList+
                ",singleRow="+isSingleRow()+")";
    }

    @Override public void setRowCount(double outerRows){
        this.numRows = outerRows > 1 ? outerRows : 1;
    }
    @Override public void setRemoteCost(double remoteCost){ this.remoteCost = remoteCost; }
    @Override public void setSingleScanRowCount(double singleRowScanCount){this.singleScanRowCount = singleRowScanCount;}
    @Override public void setNumPartitions(int numPartitions){ this.numPartitions = numPartitions; }
    @Override public double rowCount(){ return numRows; }
    @Override public double singleScanRowCount(){ return singleScanRowCount; }
    @Override public int partitionCount(){ return numPartitions; }
    @Override public double remoteCost(){ return remoteCost; }
    @Override public double localCost(){ return localCost; }
    @Override public RowOrdering getRowOrdering(){ return rowOrdering; }
    @Override public void setRowOrdering(RowOrdering rowOrdering){
        this.rowOrdering = rowOrdering;
    }
    @Override public OptimizablePredicateList getPredicateList() {return predicateList;}
    @Override
    public void setPredicateList(OptimizablePredicateList predicateList){
        this.predicateList = new PredicateList();
        if(predicateList != null) {
            for (int i = 0; i < predicateList.size(); ++i) {
                this.predicateList.addOptPredicate(predicateList.getOptPredicate(i));
            }
        }
    }
    @Override public CostEstimate getBase(){ return baseCost==null?this:baseCost; }
    @Override public void setBase(CostEstimate baseCost){ this.baseCost = (SimpleCostEstimate) baseCost; }
    @Override public long getEstimatedRowCount(){
        return Math.round(numRows);
    }
    @Override public void setEstimatedCost(double cost){ this.localCost = cost; }
    @Override public void setLocalCost(double remoteCost){ this.localCost = remoteCost; }

    @Override
    public void setEstimatedRowCount(long count){
        this.numRows = count>1?count:1;
        this.singleScanRowCount = this.numRows;
    }

    @Override
    public double getEstimatedCost(){
        return localCost + remoteCost;
    }

    @Override
    public CostEstimate cloneMe(){
        RowOrdering roClone = null;
        if(this.rowOrdering!=null){
            roClone =this.rowOrdering.getClone();
        }
        SimpleCostEstimate clone=new SimpleCostEstimate(localCost,remoteCost,numRows,singleScanRowCount,numPartitions);
        clone.isAntiJoin = this.isAntiJoin;
        clone.isOuterJoin = this.isOuterJoin();
        clone.setRowOrdering(roClone);
        clone.setPredicateList(predicateList);
        if(baseCost!=null)
            clone.setBase(baseCost.cloneMe());
        clone.isRealCost = this.isRealCost;
        clone.setEstimatedHeapSize(estimatedHeapSize);
        clone.setOpenCost(openCost);
        clone.setCloseCost(closeCost);
        clone.setFromBaseTableCost(fromBaseTableCost);
        clone.setFromBaseTableRows(fromBaseTableRows);
        clone.setIndexLookupCost(indexLookupCost);
        clone.setIndexLookupRows(indexLookupRows);
        clone.setProjectionCost(projectionCost);
        clone.setProjectionRows(projectionRows);
        clone.setScannedBaseTableRows(scannedBaseTableRows);
        clone.setLocalCostPerPartition(localCostPerPartition);
        clone.setRemoteCostPerPartition(remoteCostPerPartition);
        clone.setSingleRow(singleRow);
        return clone;
    }

    @Override
    public boolean isUninitialized(){
        return localCost == Double.MAX_VALUE &&
                numRows == Double.MAX_VALUE &&
                singleScanRowCount == Double.MAX_VALUE;
    }

    @Override
    public double compare(CostEstimate other){
        assert other!=null: "Cannot compare with a null CostEstimate";

        double thisCost=this.getEstimatedCost();
        double otherCost=other.getEstimatedCost();
        if((thisCost!=Double.POSITIVE_INFINITY) || (otherCost!= Double.POSITIVE_INFINITY)){
            return thisCost-otherCost;
        }

        if((this.numRows !=Double.POSITIVE_INFINITY)|| (other.rowCount()!=Double.POSITIVE_INFINITY)){
            return this.numRows-other.rowCount();
        }

        if((this.singleScanRowCount != Double.POSITIVE_INFINITY)||
                (other.singleScanRowCount() != Double.POSITIVE_INFINITY)){
            return this.singleScanRowCount - other.singleScanRowCount();
        }
        return 0.0d;
    }

    @Override
    public double compareLocal(CostEstimate other){
        assert other!=null: "Cannot compare with a null CostEstimate";

        double thisCost=this.localCost();
        double otherCost=other.localCost();

        if((thisCost!=Double.POSITIVE_INFINITY) || (otherCost!= Double.POSITIVE_INFINITY)){
            return thisCost-otherCost;
        }

        if((this.numRows !=Double.POSITIVE_INFINITY)|| (other.rowCount()!=Double.POSITIVE_INFINITY)){
            return this.numRows-other.rowCount();
        }

        if((this.singleScanRowCount != Double.POSITIVE_INFINITY)||
                (other.singleScanRowCount() != Double.POSITIVE_INFINITY)){
            return this.singleScanRowCount - other.singleScanRowCount();
        }
        return 0.0d;
    }

    @Override
    public CostEstimate add(CostEstimate addend,CostEstimate retval){
        assert addend!=null: "Cannot add a null cost estimate";

        double sumLocalCost = addend.localCost()+localCost;
        double sumRemoteCost = addend.remoteCost()+remoteCost;
        double sumRemoteCostPerPartition = addend.getRemoteCostPerPartition()+remoteCostPerPartition;
        double rowCount = addend.rowCount()+numRows;

        if(retval==null)
            retval = new SimpleCostEstimate();

        retval.setRemoteCost(sumRemoteCost);
        retval.setRemoteCostPerPartition(sumRemoteCostPerPartition);
        retval.setCost(sumLocalCost,rowCount,singleScanRowCount,numPartitions);
        retval.setEstimatedHeapSize(estimatedHeapSize+addend.getEstimatedHeapSize());
        return retval;
    }

    @Override
    public boolean isRealCost(){
        return isRealCost;
    }

    @Override
    public void setIsRealCost(boolean isRealCost){
        this.isRealCost = isRealCost;
    }

    @Override
    public CostEstimate multiply(double multiplicand,CostEstimate retval){

        double multLocalCost = localCost*multiplicand;
        double multRemoteCost = remoteCost*multiplicand;
        double multRemoteCostPerPartition = remoteCostPerPartition*multiplicand;
        double rowCount = numRows*multiplicand;

        if(retval==null)
            retval = new SimpleCostEstimate();

        retval.setRemoteCost(multRemoteCost);
        retval.setRemoteCostPerPartition(multRemoteCostPerPartition);
        retval.setCost(multLocalCost,rowCount,singleScanRowCount,numPartitions);
        retval.setEstimatedHeapSize((long)(estimatedHeapSize*multiplicand));
        return retval;
    }

    @Override
    public CostEstimate divide(double divisor,CostEstimate retval){
        double dividedLocalCost = localCost/divisor;
        double dividedRemoteCost = remoteCost/divisor;
        double dividedRemoteCostPerPartition = remoteCostPerPartition/divisor;
        double rowCount = numRows/divisor;

        if(retval==null)
            retval = new SimpleCostEstimate();

        retval.setRemoteCost(dividedRemoteCost);
        retval.setRemoteCostPerPartition(dividedRemoteCostPerPartition);
        retval.setCost(dividedLocalCost,rowCount,singleScanRowCount,numPartitions);
        retval.setEstimatedHeapSize((long)(estimatedHeapSize/divisor));
        return retval;
    }

    public long getEstimatedHeapSize(){
        return estimatedHeapSize;
    }

    public void setEstimatedHeapSize(long estHeapSize){
        this.estimatedHeapSize = estHeapSize;
    }

    @Override
    public boolean isOuterJoin() {
        return isOuterJoin;
    }

    @Override
    public void setOuterJoin(boolean isOuterJoin) {
        this.isOuterJoin = isOuterJoin;
    }

    @Override
    public boolean isAntiJoin() {
        return isAntiJoin;
    }

    @Override
    public void setAntiJoin(boolean isAntiJoin) {
        this.isAntiJoin = isAntiJoin;
    }

    public double getProjectionRows() {
        return getBaseCostInternal().projectionRows == -1.0d?getBaseCostInternal().getEstimatedRowCount():getBaseCostInternal().projectionRows;
    }

    public void setProjectionRows(double projectionRows) {
        this.projectionRows = projectionRows == 0.0d?1.0d:projectionRows;
    }

    public double getProjectionCost() {
        return getBaseCostInternal().projectionCost == -1.0d?getBaseCostInternal().localCost:getBaseCostInternal().projectionCost;
    }

    public void setProjectionCost(double projectionCost) {
        this.projectionCost = projectionCost;
    }

    public double getIndexLookupRows() {
        return getBaseCostInternal().indexLookupRows == -1.0d?getBaseCostInternal().getEstimatedRowCount():getBaseCostInternal().indexLookupRows;
    }

    public void setIndexLookupRows(double indexLookupRows) {
        this.indexLookupRows = indexLookupRows == 0.0d?1.0d:indexLookupRows;
    }

    public double getIndexLookupCost() {
        return getBaseCostInternal().indexLookupCost == -1.0d?getBaseCostInternal().getLocalCost():getBaseCostInternal().indexLookupCost;
    }

    public void setIndexLookupCost(double indexLookupCost) {
        this.indexLookupCost = indexLookupCost;
    }

    public double getFromBaseTableRows() {
        return getBaseCostInternal().fromBaseTableRows == -1.0d?getBaseCostInternal().getEstimatedRowCount():getBaseCostInternal().fromBaseTableRows;
    }

    public void setFromBaseTableRows(double fromBaseTableRows) {
        this.fromBaseTableRows = fromBaseTableRows == 0.0d?1.0d:fromBaseTableRows;
    }

    public double getScannedBaseTableRows() {
        return getBaseCostInternal().scannedBaseTableRows == -1.0d?getBaseCostInternal().getEstimatedRowCount():getBaseCostInternal().scannedBaseTableRows;
    }

    public void setScannedBaseTableRows(double scannedBaseTableRows) {
        this.scannedBaseTableRows = scannedBaseTableRows == 0.0d?1.0d:scannedBaseTableRows;
    }

    public double getFromBaseTableCost() {
        return getBaseCostInternal().fromBaseTableCost == -1.0d?getBaseCostInternal().getLocalCost():getBaseCostInternal().fromBaseTableCost;
    }

    public void setFromBaseTableCost(double fromBaseTableCost) {
        this.fromBaseTableCost = fromBaseTableCost;
    }

    @Override
    public double getLocalCost() {
        return localCost;
    }

    @Override
    public double getRemoteCost() {
        return remoteCost;
    }

    private SimpleCostEstimate getBaseCostInternal() {
        return (SimpleCostEstimate) getBase();
    }

    @Override
    public double getLocalCostPerPartition() {
        return localCostPerPartition;
    }

    @Override
    public void setLocalCostPerPartition(double localCostPerPartition) {
        this.localCostPerPartition = localCostPerPartition;
    }

    @Override
    public void setLocalCostPerPartition(double localCost, int numPartitions) {
        if (numPartitions <= 0)
            numPartitions = 1;
        setLocalCostPerPartition(localCost / numPartitions);
    }

    @Override
    public void setRemoteCostPerPartition(double remoteCost, int numPartitions) {
        if (numPartitions <= 0)
            numPartitions = 1;
        setRemoteCostPerPartition(remoteCost / numPartitions);
    }

    @Override
    public double getRemoteCostPerPartition() {
        return remoteCostPerPartition;
    }

    @Override
    public void setRemoteCostPerPartition(double remoteCostPerPartition) {
        this.remoteCostPerPartition = remoteCostPerPartition;
    }

    @Override
    public double getAccumulatedMemory() {
        return accumulatedMemory;
    }

    @Override
    public void setAccumulatedMemory(double memorySize) {
        accumulatedMemory = memorySize;
    }

    public boolean isSingleRow() {return singleRow;}

    public void setSingleRow(boolean singleRowInRelation) { singleRow = singleRowInRelation;}
}
