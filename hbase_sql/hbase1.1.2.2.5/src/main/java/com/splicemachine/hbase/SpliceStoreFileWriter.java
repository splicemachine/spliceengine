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

package com.splicemachine.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by jyuan on 4/8/19.
 */
public class SpliceStoreFileWriter {

    private StoreFile.Writer writer;
    public static final byte[] BULKLOAD_TIME_KEY = StoreFile.BULKLOAD_TIME_KEY;
    public static final byte[] BULKLOAD_TASK_KEY = StoreFile.BULKLOAD_TASK_KEY;
    public static final byte[] MAJOR_COMPACTION_KEY = StoreFile.MAJOR_COMPACTION_KEY;
    public static final byte[] EXCLUDE_FROM_MINOR_COMPACTION_KEY = StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY;

    public SpliceStoreFileWriter(StoreFile.Writer writer) {
        this.writer = writer;
    }

    public Path getPath() {
        return this.writer.getPath();
    }

    public void appendFileInfo(byte[] key, byte[] value) throws IOException {
        writer.appendFileInfo(key, value);
    }

    public void appendTrackedTimestampsToMetadata() throws IOException {
        writer.appendTrackedTimestampsToMetadata();
    }

    public void close() throws IOException {
        writer.close();
    }

    public void append(final Cell cell) throws IOException {
        writer.append(cell);
    }

    public static class Builder {
        private StoreFile.WriterBuilder builder;

        public Builder(Configuration conf, CacheConfig cacheConf, FileSystem fs) {
            builder = new StoreFile.WriterBuilder(conf, cacheConf, fs);
        }

        public Builder withFilePath(Path path) {
            builder.withFilePath(path);
            return this;
        }

        public Builder withOutputDir(Path dir) {
            builder.withOutputDir(dir);
            return this;
        }

        public Builder withBloomType(BloomType bloomType) {
            builder.withBloomType(bloomType);
            return this;
        }

        public Builder withFileContext(HFileContext fileContext) {
            builder.withFileContext(fileContext);
            return this;
        }

        public Builder withFavoredNodes(InetSocketAddress[] favoredNodes) {
            builder.withFavoredNodes(favoredNodes);
            return this;
        }

        public SpliceStoreFileWriter build() throws IOException {
            return new SpliceStoreFileWriter(builder.build());
        }
    }
}
