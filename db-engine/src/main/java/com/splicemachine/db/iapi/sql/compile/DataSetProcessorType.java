package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;

public enum DataSetProcessorType {
    DEFAULT_CONTROL,
    COST_SUGGESTED_SPARK,
    SESSION_HINTED_CONTROL,
    SESSION_HINTED_SPARK,
    QUERY_HINTED_CONTROL,
    QUERY_HINTED_SPARK,
    FORCED_CONTROL,
    FORCED_SPARK;

    public boolean isSpark() {
        switch(this) {
            case SESSION_HINTED_SPARK:
            case QUERY_HINTED_SPARK:
            case COST_SUGGESTED_SPARK:
            case FORCED_SPARK:
                return true;
        }
        return false;
    }

    public String level() {
        switch(this) {
            case FORCED_SPARK:
            case FORCED_CONTROL:
                return "forced";
            case QUERY_HINTED_SPARK:
            case QUERY_HINTED_CONTROL:
                return "query hint";
            case SESSION_HINTED_SPARK:
            case SESSION_HINTED_CONTROL:
                return "session hint";
            case COST_SUGGESTED_SPARK:
                return "cost";
            case DEFAULT_CONTROL:
                return "default";
        }
        assert false;
        return "";
    }

    public boolean isForced() {
        return this == FORCED_CONTROL || this == FORCED_SPARK;
    }

    public boolean isQueryHinted() {
        return this == QUERY_HINTED_SPARK || this == QUERY_HINTED_CONTROL;
    }

    public boolean isSessionHinted() {
        return this == SESSION_HINTED_SPARK || this == SESSION_HINTED_CONTROL;
    }

    public boolean isHinted() {
        return this.isQueryHinted() || isSessionHinted();
    }

    public boolean isDefaultControl() {
        return this == DEFAULT_CONTROL;
    }

    public DataSetProcessorType combine(DataSetProcessorType other) throws StandardException {
        if (other == null) {
            return this;
        }
        switch(this) {
            case FORCED_SPARK:
            case FORCED_CONTROL:
                if (other.isForced() && other != this) {
                    throw StandardException.newException(SQLState.LANG_INVALID_SPARK_AND_CONTROL, level());
                }
                return this;
            case QUERY_HINTED_CONTROL:
            case QUERY_HINTED_SPARK:
                if (other.isForced()) {
                    return other;
                } else if (other.isQueryHinted() && other != this) {
                    throw StandardException.newException(SQLState.LANG_INVALID_SPARK_AND_CONTROL, level());
                }
                return this;
            case SESSION_HINTED_CONTROL:
            case SESSION_HINTED_SPARK:
                if (other.isForced() || other.isQueryHinted()) {
                    return other;
                } else if (other.isSessionHinted() && other != this) {
                    throw StandardException.newException(SQLState.LANG_INVALID_SPARK_AND_CONTROL, level());
                }
                return this;
            case COST_SUGGESTED_SPARK:
                if (other != DEFAULT_CONTROL) {
                    return other;
                }
                return this;
            case DEFAULT_CONTROL:
                return other;
        }
        assert false;
        return this;
    }
}
