package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;

public enum DataSetProcessorType {
    DEFAULT_OLTP,
    COST_SUGGESTED_OLAP,
    SESSION_HINTED_OLTP,
    SESSION_HINTED_OLAP,
    QUERY_HINTED_OLTP,
    QUERY_HINTED_OLAP,
    FORCED_OLTP,
    FORCED_OLAP;

    public boolean isOlap() {
        switch(this) {
            case SESSION_HINTED_OLAP:
            case QUERY_HINTED_OLAP:
            case COST_SUGGESTED_OLAP:
            case FORCED_OLAP:
                return true;
        }
        return false;
    }

    public String level() {
        switch(this) {
            case FORCED_OLAP:
            case FORCED_OLTP:
                return "forced";
            case QUERY_HINTED_OLAP:
            case QUERY_HINTED_OLTP:
                return "query hint";
            case SESSION_HINTED_OLAP:
            case SESSION_HINTED_OLTP:
                return "session hint";
            case COST_SUGGESTED_OLAP:
                return "cost";
            case DEFAULT_OLTP:
                return "default";
        }
        assert false;
        return "";
    }

    public boolean isForced() {
        return this == FORCED_OLTP || this == FORCED_OLAP;
    }

    public boolean isQueryHinted() {
        return this == QUERY_HINTED_OLAP || this == QUERY_HINTED_OLTP;
    }

    public boolean isSessionHinted() {
        return this == SESSION_HINTED_OLAP || this == SESSION_HINTED_OLTP;
    }

    public boolean isHinted() {
        return this.isQueryHinted() || isSessionHinted();
    }

    public boolean isDefaultOltp() {
        return this == DEFAULT_OLTP;
    }

    public DataSetProcessorType combine(DataSetProcessorType other) throws StandardException {
        if (other == null) {
            return this;
        }
        switch(this) {
            case FORCED_OLAP:
            case FORCED_OLTP:
                if (other.isForced() && other != this) {
                    throw StandardException.newException(SQLState.LANG_INVALID_OLAP_AND_OLTP, level());
                }
                return this;
            case QUERY_HINTED_OLTP:
            case QUERY_HINTED_OLAP:
                if (other.isForced()) {
                    return other;
                } else if (other.isQueryHinted() && other != this) {
                    throw StandardException.newException(SQLState.LANG_INVALID_OLAP_AND_OLTP, level());
                }
                return this;
            case SESSION_HINTED_OLTP:
            case SESSION_HINTED_OLAP:
                if (other.isForced() || other.isQueryHinted()) {
                    return other;
                } else if (other.isSessionHinted() && other != this) {
                    throw StandardException.newException(SQLState.LANG_INVALID_OLAP_AND_OLTP, level());
                }
                return this;
            case COST_SUGGESTED_OLAP:
                if (other != DEFAULT_OLTP) {
                    return other;
                }
                return this;
            case DEFAULT_OLTP:
                return other;
        }
        assert false;
        return this;
    }
}
