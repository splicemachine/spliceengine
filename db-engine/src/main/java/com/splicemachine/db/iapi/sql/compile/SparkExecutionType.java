package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;

public enum SparkExecutionType {
    UNSPECIFIED,
    SESSION_HINTED_NATIVE,
    SESSION_HINTED_NON_NATIVE,
    QUERY_HINTED_NATIVE,
    QUERY_HINTED_NON_NATIVE,
    FORCED_NATIVE,
    FORCED_NON_NATIVE;

    public boolean isNative() {
        switch(this) {
            case SESSION_HINTED_NATIVE:
            case QUERY_HINTED_NATIVE:
            case FORCED_NATIVE:
                return true;
        }
        return false;
    }

    public boolean isNonNative() {
        switch(this) {
            case SESSION_HINTED_NON_NATIVE:
            case QUERY_HINTED_NON_NATIVE:
            case FORCED_NON_NATIVE:
                return true;
        }
        return false;
    }

    public String level() {
        switch(this) {
            case FORCED_NATIVE:
            case FORCED_NON_NATIVE:
                return "forced";
            case QUERY_HINTED_NATIVE:
            case QUERY_HINTED_NON_NATIVE:
                return "query hint";
            case SESSION_HINTED_NATIVE:
            case SESSION_HINTED_NON_NATIVE:
                return "session hint";
            case UNSPECIFIED:
                return "unspecified";
        }
        assert false;
        return "";
    }

    public boolean isForced() {
        return this == FORCED_NON_NATIVE || this == FORCED_NATIVE;
    }

    public boolean isQueryHinted() {
        return this == QUERY_HINTED_NATIVE || this == QUERY_HINTED_NON_NATIVE;
    }

    public boolean isSessionHinted() {
        return this == SESSION_HINTED_NATIVE|| this == SESSION_HINTED_NON_NATIVE;
    }

    public boolean isHinted() {
        return this.isQueryHinted() || isSessionHinted();
    }

    public boolean isUnspecified() {
        return this == UNSPECIFIED;
    }

    public SparkExecutionType combine(SparkExecutionType other) throws StandardException {
        if (other == null) {
            return this;
        }
        switch(this) {
            case FORCED_NATIVE:
            case FORCED_NON_NATIVE:
                if (other.isForced() && other != this) {
                    throw StandardException.newException(SQLState.LANG_INVALID_NATIVE_AND_NON_NATIVE_SPARK, level());
                }
                return this;
            case QUERY_HINTED_NATIVE:
            case QUERY_HINTED_NON_NATIVE:
                if (other.isForced()) {
                    return other;
                } else if (other.isQueryHinted() && other != this) {
                    throw StandardException.newException(SQLState.LANG_INVALID_NATIVE_AND_NON_NATIVE_SPARK, level());
                }
                return this;
            case SESSION_HINTED_NATIVE:
            case SESSION_HINTED_NON_NATIVE:
                if (other.isForced() || other.isQueryHinted()) {
                    return other;
                } else if (other.isSessionHinted() && other != this) {
                    throw StandardException.newException(SQLState.LANG_INVALID_NATIVE_AND_NON_NATIVE_SPARK, level());
                }
                return this;
            case UNSPECIFIED:
                return other;
        }
        assert false;
        return this;
    }
}
