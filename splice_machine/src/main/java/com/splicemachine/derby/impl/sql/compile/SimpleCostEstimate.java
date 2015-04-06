package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.RowOrdering;
import com.splicemachine.db.iapi.store.access.SortCostController;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.store.access.StoreCostResult;
import com.splicemachine.db.impl.sql.compile.RowOrderingImpl;

import java.text.DecimalFormat;

/**
 * @author Scott Fines
 *         Date: 3/13/15
 */
public class SimpleCostEstimate implements CostEstimate{
    private double localCost = Double.MAX_VALUE;
    private double remoteCost;
    private int numPartitions;
    private double numRows = Double.MAX_VALUE;

    private double singleScanRowCount = Double.MAX_VALUE;
    private RowOrdering rowOrdering;

    private CostEstimate baseCost;
    private boolean isRealCost=true;
    private StoreCostController storeCost;
    private SortCostController sortCost;
    private long estimatedHeapSize;

    private double openCost;
    private double closeCost;

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

    public SimpleCostEstimate(double localCost,
                              double remoteCost,
                              double numRows,
                              double singleScanRowCount,
                              int numPartitions,
                              CostEstimate base){
        this(localCost, remoteCost, numRows, singleScanRowCount, numPartitions);
        this.baseCost = base;
    }

    public void setStoreCost(StoreCostController storeCost){
        this.storeCost = storeCost;
    }

    public StoreCostController getStoreCost(){
        return storeCost;
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

        CostEstimate base=other.getBase();
        if(base!=null && base != other)
            this.baseCost = base.cloneMe();
    }


    @Override
    public String prettyString(){
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(3);
        df.setGroupingUsed(false);

        return "(overallCost="+df.format(getEstimatedCost())
                +",localCost="+df.format(localCost())
                +",remoteCost="+df.format(remoteCost())
                +",outputRows="+getEstimatedRowCount()
                +",outputHeapSize="+getEstimatedHeapSize()+
                ",partitions="+partitionCount()+")";
    }

    @Override
    public String toString(){
        return "(overallCost="+getEstimatedCost()
                +",localCost="+localCost()
                +",remoteCost="+remoteCost()
                +",outputRows="+getEstimatedRowCount()
                +",outputHeapSize="+getEstimatedHeapSize()+
                ",partitions="+partitionCount()+")";
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
    @Override public void setRowOrdering(RowOrdering rowOrdering){ this.rowOrdering = rowOrdering; }
    @Override public CostEstimate getBase(){ return baseCost==null?this:baseCost; }
    @Override public void setBase(CostEstimate baseCost){ this.baseCost = baseCost; }
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
        RowOrdering roClone = new RowOrderingImpl();
        if(this.rowOrdering!=null)
           this.rowOrdering.copy(roClone);
        SimpleCostEstimate clone=new SimpleCostEstimate(localCost,remoteCost,numRows,singleScanRowCount,numPartitions);
        clone.setRowOrdering(rowOrdering);
        clone.setEstimatedHeapSize(estimatedHeapSize);
        clone.setOpenCost(openCost);
        clone.setCloseCost(closeCost);
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
}
