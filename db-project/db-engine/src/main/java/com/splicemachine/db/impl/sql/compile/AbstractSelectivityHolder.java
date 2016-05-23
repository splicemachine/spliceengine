package com.splicemachine.db.impl.sql.compile;

/**
 *
 * Abstract class handling comparisons, phase and colNum setting.
 *
 */
public abstract class AbstractSelectivityHolder implements SelectivityHolder {
    protected QualifierPhase phase;
    protected int colNum;
    protected double selectivity = -1.0d;

    public AbstractSelectivityHolder(int colNum, QualifierPhase phase) {
        this.phase = phase;
        this.colNum = colNum;
    }
    public int getColNum () {
        return colNum;
    }

    public QualifierPhase getPhase () {
        return phase;
    }
    public boolean isRangeSelectivity() {
        return false;
    }

    @Override
    public int compareTo(SelectivityHolder o) {
        try {
            return Double.compare(getSelectivity(), o.getSelectivity());
        } catch (Exception e) {
            throw new RuntimeException("selectivity holder comparison failed",e);
        }
    }
}
