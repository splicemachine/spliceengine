/*
 * Copyright (c) 2021 Splice Machine, Inc.
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

package com.splicemachine.db.iapi.reference;

import com.splicemachine.db.shared.common.sql.Utils;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

public class GlobalDBProperties {
    public interface Validator {
        void accept(String t) throws Throwable;
    }

    public static class PropertyType {
        String name, information;
        Validator validator;
        public PropertyType(String name, String information, Validator validator) {
            this.name = name;
            this.information = information;
            this.validator = validator;
        }

        public String validate(String s) {
            if(validator == null || s == null) return "";
            try {
                validator.accept(s);
            } catch (Throwable e) {
                return "Error parsing '" + s + "' for option " + name + ": " + e.toString();
            }
            return "";
        }

        public String getName() {
            return name;
        }

        public String getInformation() {
            return information;
        }

        @Override
        public String toString() { return name; }
    }

    public static void parseBoolean(String s) {
        if( s == null || (!s.equalsIgnoreCase("false") && !s.equalsIgnoreCase("true")) ) {
            throw new RuntimeException("Expected either TRUE or FALSE.");
        }
    }

    static class MultipleOptionsValidator implements Validator {
        String[] options;
        MultipleOptionsValidator(String[] options) {
            this.options = options;
        }

        @Override
        public void accept(String t) throws Throwable {
            if( Arrays.stream(options).filter( t::equalsIgnoreCase ).count() != 0)
                return;
            throw new RuntimeException("Supported values are " + Arrays.toString(options) + ".");
        }
    }

    public static Stream<PropertyType> getAll() {
        return Arrays.stream(GlobalDBProperties.class.getFields()).map(
                field -> {
                    if (!field.getType().equals(GlobalDBProperties.PropertyType.class)) return null;
                    try {
                        return ((PropertyType) field.get(null));
                    } catch (IllegalAccessException e) {
                        return null;
                    }
                } ).filter( f -> f != null );
    }


    public static PropertyType SPLICE_TIMESTAMP_FORMAT =
            new PropertyType("splice.function.timestampFormat", "format for timestamps when cast to string on the server",
                    Utils::getTimestampFormatLength);
    public static PropertyType PRESERVE_LINE_ENDINGS =
            new PropertyType("splice.function.preserveLineEndings", "", GlobalDBProperties::parseBoolean);

    public static PropertyType SPLICE_CURRENT_TIMESTAMP_PRECISION =
            new PropertyType("splice.function.currentTimestampPrecision",
                    "Fractional seconds precision of current_timestamp",
                    Integer::parseInt);

    public static PropertyType FLOATING_POINT_NOTATION =
            new PropertyType("splice.function.floatingPointNotation", "notation for floating point values",
                    new MultipleOptionsValidator(new String[]{"plain", "normalized", "default"}));

    public static PropertyType SPLICE_OLAP_PARALLEL_PARTITIONS =
            new PropertyType("splice.olapParallelPartitions", "", Integer::parseInt);

    public static PropertyType SPLICE_DB2_ERROR_COMPATIBLE =
            new PropertyType("splice.db2.error.compatible",
                    "if true, use db2-compatible error codes", GlobalDBProperties::parseBoolean);

    // if set to true, will treat "" as empty string in IMPORT_DATA
    // if set to false or NULL, will treat "" as NULL in IMPORT_DATA
    public static PropertyType SPLICE_DB2_IMPORT_EMPTY_STRING_COMPATIBLE =
            new PropertyType("splice.db2.import.empty_string_compatible",
                    "if true, read \"\" as empty string, not NULL", GlobalDBProperties::parseBoolean);

    public static PropertyType SPLICE_DB2_VARCHAR_COMPATIBLE =
            new PropertyType("splice.db2.varchar.compatible",
                    "if true, ignore trailing spaces in varchar comparisons",
                    GlobalDBProperties::parseBoolean);

    /**
     * If enabled, disable calculation of join costs as cost per parallel task
     * and revert to using the old units: cost per partition (cost per region).
     */
    public static PropertyType DISABLE_PARALLEL_TASKS_JOIN_COSTING =
            new PropertyType("splice.optimizer.disablePerParallelTaskJoinCosting",
                    "if true, disable calculation of join costs as cost per parallel task, " +
                            "and use costs per partition",
                    GlobalDBProperties::parseBoolean);

    /**
     * @sa com.splicemachine.db.iapi.sql.compile.CompilerContext . NewMergeJoinExecutionType
     */
    public static PropertyType SPLICE_NEW_MERGE_JOIN =
            new PropertyType("splice.execution.newMergeJoin",
                    "use new merge join. possible values: on, off, forced",
                    new MultipleOptionsValidator(new String[]{"on", "off", "forced"}));
}
