package com.splicemachine.utils.file;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * This is a handy abstraction for dealing with the different versions of hbase/hadoop,
 * as well as different distributions. Provides utilities for ensuring that the proper behavior
 * works in most cases.
 *
 * At the moment, it appeals to the lowest common denominator, although eventually we'll
 * implement it differently depending on different compile targets.
 *
 * @author Scott Fines
 * Date: 3/13/14
 */
public interface FileInfo {

		public boolean isWritable(Path path) throws IOException;

		public boolean isReadable(Path path) throws IOException;

		public boolean isDirectory(Path path) throws IOException;

		public String[] getUserAndGroup() throws IOException;
}
