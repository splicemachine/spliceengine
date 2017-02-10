/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.spark;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import java.io.IOException;
import java.io.InputStream;

public class WholeTextInputFormat extends CombineFileInputFormat<String, InputStream> implements Configurable {

    private Configuration conf;

    public WholeTextInputFormat() {
    }

    @Override
    public RecordReader<String, InputStream> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        taskAttemptContext.setStatus(inputSplit.toString());
        return new StringInputStreamRecordReader((CombineFileSplit) inputSplit, taskAttemptContext);
    }

    @Override
    public void setConf(Configuration configuration) {
        conf = configuration;
        setMaxSplitSize(1); // make sure we create a split per file, rather than merging files into a single split
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    private class StringInputStreamRecordReader extends RecordReader<String, InputStream> {
        private String key;
        private InputStream value;
        private FileSystem fs;
        private CombineFileSplit split;

        private int currentPath = 0;

        StringInputStreamRecordReader(CombineFileSplit inputSplit,TaskAttemptContext taskAttemptContext) {
            this.split = inputSplit;
            Path path = split.getPath(0);
            try {
                this.fs = path.getFileSystem(taskAttemptContext.getConfiguration());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (currentPath>=split.getNumPaths()) {
                return false;
            }

            Path path = split.getPath(currentPath);
            currentPath++;

            CompressionCodecFactory factory = new CompressionCodecFactory(conf);
            CompressionCodec codec = factory.getCodec(path);
            key = path.toString();
            FSDataInputStream fileIn = fs.open(path);

            value = codec!=null?codec.createInputStream(fileIn):fileIn;
            return true;
        }

        @Override
        public String getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public InputStream getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return ((float)currentPath)/split.getNumPaths();
        }

        @Override
        public void close() throws IOException {

        }
    }
}