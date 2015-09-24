package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicateList;
import com.splicemachine.db.iapi.sql.compile.RowOrdering;
import com.splicemachine.db.impl.sql.compile.PredicateList;
import com.splicemachine.db.iapi.error.StandardException;
import java.text.DecimalFormat;

/**
 * @author Scott Fines
 *         Date: 3/13/15
 */
public class SimpleCostEstimate implements CostEstimate{
    private static final String[] displayHeapUnits = new String[]{" B"," KB"," MB"," GB"," TB"};
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


    public SimpleCostEstimate(){ }

    public SimpleCostEstimate(double theCost,double theRowCount,double theSingleScanRowCount){
        this(theCost,0d,theRowCount,theSingleScanRowCount,1);
    }

    public SimpleCostEstimate(double localCost,double remoteCost,double numRows,double singleScanRowCount,int numPartitions){
        this.localCost=localCost;
        this.remoteCost=remoteCost;
        this.numRows=numRows;
        this.singleScanRowCount=singleScanRowCount;
        this.numPartitions=numPartitions;
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
        this.numRows = rowCount;
        this.singleScanRowCount = singleScanRowCount;
        this.numPartitions = numPartitions;
    }

    @Override
    public void setCost(CostEstimate other){
        this.localCost = other.localCost();
        this.remoteCost = other.remoteCost();
        this.numPartitions =other.partitionCount();
        this.numRows = other.rowCount();
        this.singleScanRowCount = other.singleScanRowCount();
        this.estimatedHeapSize = other.getEstimatedHeapSize();
        this.openCost = other.getOpenCost();
        this.closeCost = other.getCloseCost();
        RowOrdering rowOrdering=other.getRowOrdering();
        if(rowOrdering!=null)
            this.rowOrdering = rowOrdering.getClone();
        else
            this.rowOrdering = null;

        setPredicateList(other.getPredicateList());
        CostEstimate base=other.getBase();
        if(base!=null && base != other)
            this.baseCost = (SimpleCostEstimate) base.cloneMe();
    }

    @Override
    public String prettyProcessingString(){
        return prettyStringOutput(localCost, getEstimatedRowCount());
    }

    @Override
    public String prettyFromBaseTableString() {
        return prettyStringOutput(getFromBaseTableCost(), (long) Math.round(getFromBaseTableRows()));
    }

    @Override
    public String prettyIndexLookupString() {
        return prettyStringOutput(getIndexLookupCost(), (long) Math.round(getIndexLookupRows()));
    }

    private String prettyStringOutput(double cost,long rows) {
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

        return "totalCost="+df.format(cost/1000)
                +",outputRows="+rows
                +",outputHeapSize="+df.format(eHeap)+unit
                +",partitions="+partitionCount();
    }

    @Override
    public String prettyProjectionString() {
        return prettyStringOutput(getProjectionCost(), (long) Math.round(getProjectionRows()));
    }

    @Override
    public String prettyScrollInsensitiveString(){
        return prettyStringOutput(getEstimatedCost(), getEstimatedRowCount());
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
                ",predicateList="+predicateList+")";
    }

    @Override public void setRowCount(double outerRows){ this.numRows = outerRows;  }
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
        this.numRows = count;
        this.singleScanRowCount = count;
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
        clone.setRowOrdering(roClone);
        clone.setPredicateList(predicateList);
        clone.setEstimatedHeapSize(estimatedHeapSize);
        clone.setOpenCost(openCost);
        clone.setCloseCost(closeCost);
        clone.setFromBaseTableCost(fromBaseTableCost);
        clone.setFromBaseTableRows(fromBaseTableRows);
        clone.setIndexLookupCost(indexLookupCost);
        clone.setIndexLookupRows(indexLookupRows);
        if(baseCost!=null)
            clone.setBase(baseCost.cloneMe());
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
    public CostEstimate add(CostEstimate addend,CostEstimate retval){
        assert addend!=null: "Cannot add a null cost estimate";

        double sumLocalCost = addend.localCost()+localCost;
        double sumRemoteCost = addend.remoteCost()+remoteCost;
        double rowCount = addend.rowCount()+numRows;

        if(retval==null)
            retval = new SimpleCostEstimate();

        retval.setRemoteCost(sumRemoteCost);
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
        double rowCount = numRows*multiplicand;

        if(retval==null)
            retval = new SimpleCostEstimate();

        retval.setRemoteCost(multRemoteCost);
        retval.setCost(multLocalCost,rowCount,singleScanRowCount,numPartitions);
        retval.setEstimatedHeapSize((long)(estimatedHeapSize*multiplicand));
        return retval;
    }

    @Override
    public CostEstimate divide(double divisor,CostEstimate retval){
        double multLocalCost = localCost/divisor;
        double multRemoteCost = remoteCost/divisor;
        double rowCount = numRows/divisor;

        if(retval==null)
            retval = new SimpleCostEstimate();

        retval.setRemoteCost(multRemoteCost);
        retval.setCost(multLocalCost,rowCount,singleScanRowCount,numPartitions);
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

}
