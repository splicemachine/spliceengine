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

package com.splicemachine.mrio.api.hive;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.security.Credentials;
/**
 * 
 * Simple Wrapper for config...
 *
 */
public class SMHIveContextWrapper implements JobContext {
	protected Configuration config;
	public SMHIveContextWrapper(Configuration config) {
		this.config = config;
	}
	
	@Override
	public Configuration getConfiguration() {
		return config;
	}

	@Override
	public Credentials getCredentials() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JobID getJobID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getNumReduceTasks() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Path getWorkingDirectory() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<?> getOutputKeyClass() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<?> getOutputValueClass() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<?> getMapOutputKeyClass() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<?> getMapOutputValueClass() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getJobName() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean userClassesTakesPrecedence() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Class<? extends InputFormat<?, ?>> getInputFormatClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<? extends Partitioner<?, ?>> getPartitionerClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RawComparator<?> getSortComparator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getJar() {
		// TODO Auto-generated method stub
		return null;
	}

	public RawComparator<?> getCombinerKeyGroupingComparator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RawComparator<?> getGroupingComparator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getJobSetupCleanupNeeded() {
		// TODO Auto-generated method stub
		return false;
	}

//	@Override
	public boolean getTaskCleanupNeeded() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean getProfileEnabled() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getProfileParams() {
		// TODO Auto-generated method stub
		return null;
	}

//	@Override
	public IntegerRanges getProfileTaskRange(boolean isMap) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getUser() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getSymlink() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Path[] getArchiveClassPaths() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public URI[] getCacheArchives() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public URI[] getCacheFiles() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path[] getLocalCacheArchives() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path[] getLocalCacheFiles() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path[] getFileClassPaths() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getArchiveTimestamps() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getFileTimestamps() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getMaxMapAttempts() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxReduceAttempts() {
		// TODO Auto-generated method stub
		return 0;
	}

}
