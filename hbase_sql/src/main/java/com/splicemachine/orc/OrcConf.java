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
package com.splicemachine.orc;

import org.apache.hadoop.conf.Configuration;

/**
 *
 *
 */
public class OrcConf {
    public OrcConf() {
    }

    public static int getIntVar(Configuration conf, OrcConf.ConfVars var) {
        return conf.getInt(var.varname, var.defaultIntVal);
    }

    public static void setIntVar(Configuration conf, OrcConf.ConfVars var, int val) {
        conf.setInt(var.varname, val);
    }

    public static long getLongVar(Configuration conf, OrcConf.ConfVars var) {
        return conf.getLong(var.varname, var.defaultLongVal);
    }

    public static void setLongVar(Configuration conf, OrcConf.ConfVars var, long val) {
        conf.setLong(var.varname, val);
    }

    public static float getFloatVar(Configuration conf, OrcConf.ConfVars var) {
        return conf.getFloat(var.varname, var.defaultFloatVal);
    }

    public static void setFloatVar(Configuration conf, OrcConf.ConfVars var, float val) {
        conf.setFloat(var.varname, val);
    }

    public static boolean getBoolVar(Configuration conf, OrcConf.ConfVars var) {
        return conf.getBoolean(var.varname, var.defaultBoolVal);
    }

    public static void setBoolVar(Configuration conf, OrcConf.ConfVars var, boolean val) {
        conf.setBoolean(var.varname, val);
    }

    public static String getVar(Configuration conf, OrcConf.ConfVars var) {
        return conf.get(var.varname, var.defaultVal);
    }

    public static void setVar(Configuration conf, OrcConf.ConfVars var, String val) {
        conf.set(var.varname, val);
    }

    public static enum ConfVars {
        HIVE_ORC_COMPRESSION("hive.exec.orc.compress", "ZLIB"),
        HIVE_ORC_ZLIB_COMPRESSION_LEVEL("hive.exec.orc.compress.zlib.level", 4),
        HIVE_ORC_COMPRESSION_BLOCK_SIZE("hive.exec.orc.compress.size", 262144),
        HIVE_ORC_STRIPE_SIZE("hive.exec.orc.stripe.size", 268435456L),
        HIVE_ORC_ROW_INDEX_STRIDE("hive.exec.orc.row.index.stride", 10000),
        HIVE_ORC_CREATE_INDEX("hive.exec.orc.create.index", true),
        HIVE_ORC_DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD("hive.exec.orc.dictionary.key.numeric.size.threshold", 0.7F),
        HIVE_ORC_DICTIONARY_STRING_KEY_SIZE_THRESHOLD("hive.exec.orc.dictionary.key.string.size.threshold", 0.8F),
        HIVE_ORC_DICTIONARY_SORT_KEYS("hive.exec.orc.dictionary.key.sorted", true),
        HIVE_ORC_BUILD_STRIDE_DICTIONARY("hive.exec.orc.build.stride.dictionary", true),
        HIVE_ORC_MAX_DICTIONARY_SIZE("hive.exec.orc.max.dictionary.size", 104857600),
        HIVE_ORC_ENTROPY_KEY_STRING_SIZE_THRESHOLD("hive.exec.orc.entropy.key.string.size.threshold", 0.9F),
        HIVE_ORC_ENTROPY_STRING_MIN_SAMPLES("hive.exec.orc.entropy.string.min.samples", 100),
        HIVE_ORC_ENTROPY_STRING_DICT_SAMPLE_FRACTION("hive.exec.orc.entropy.string.dict.sample.fraction", 0.001F),
        HIVE_ORC_ENTROPY_STRING_THRESHOLD("hive.exec.orc.entropy.string.threshold", 20),
        HIVE_ORC_DICTIONARY_ENCODING_INTERVAL("hive.exec.orc.encoding.interval", 30),
        HIVE_ORC_USE_VINTS("hive.exec.orc.use.vints", true),
        HIVE_ORC_READ_COMPRESSION_STRIDES("hive.orc.read.compression.strides", 5),
        HIVE_ORC_FILE_MEMORY_POOL("hive.exec.orc.memory.pool", 0.5F),
        HIVE_ORC_FILE_MIN_MEMORY_ALLOCATION("hive.exec.orc.min.mem.allocation", 4194304L),
        HIVE_ORC_FILE_ENABLE_LOW_MEMORY_MODE("hive.exec.orc.low.memory", false),
        HIVE_ORC_ROW_BUFFER_SIZE("hive.exec.orc.row.buffer.size", 100),
        HIVE_ORC_EAGER_HDFS_READ("hive.exec.orc.eager.hdfs.read", true),
        HIVE_ORC_EAGER_HDFS_READ_BYTES("hive.exec.orc.eager.hdfs.read.bytes", 193986560);

        public final String varname;
        public final String defaultVal;
        public final int defaultIntVal;
        public final long defaultLongVal;
        public final float defaultFloatVal;
        public final boolean defaultBoolVal;

        private ConfVars(String varname, String defaultVal) {
            this.varname = varname;
            this.defaultVal = defaultVal;
            this.defaultIntVal = -1;
            this.defaultLongVal = -1L;
            this.defaultFloatVal = -1.0F;
            this.defaultBoolVal = false;
        }

        private ConfVars(String varname, int defaultIntVal) {
            this.varname = varname;
            this.defaultVal = Integer.toString(defaultIntVal);
            this.defaultIntVal = defaultIntVal;
            this.defaultLongVal = -1L;
            this.defaultFloatVal = -1.0F;
            this.defaultBoolVal = false;
        }

        private ConfVars(String varname, long defaultLongVal) {
            this.varname = varname;
            this.defaultVal = Long.toString(defaultLongVal);
            this.defaultIntVal = -1;
            this.defaultLongVal = defaultLongVal;
            this.defaultFloatVal = -1.0F;
            this.defaultBoolVal = false;
        }

        private ConfVars(String varname, float defaultFloatVal) {
            this.varname = varname;
            this.defaultVal = Float.toString(defaultFloatVal);
            this.defaultIntVal = -1;
            this.defaultLongVal = -1L;
            this.defaultFloatVal = defaultFloatVal;
            this.defaultBoolVal = false;
        }

        private ConfVars(String varname, boolean defaultBoolVal) {
            this.varname = varname;
            this.defaultVal = Boolean.toString(defaultBoolVal);
            this.defaultIntVal = -1;
            this.defaultLongVal = -1L;
            this.defaultFloatVal = -1.0F;
            this.defaultBoolVal = defaultBoolVal;
        }
    }
}
