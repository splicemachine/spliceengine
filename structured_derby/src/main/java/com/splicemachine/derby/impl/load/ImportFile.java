package com.splicemachine.derby.impl.load;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.derby.utils.SpliceUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a "File" in the sense of BlockImports.
 *
 * In Reality, this is a list of files, each stored in HDFS (within
 * the same directory space), with
 * some convenience functions applied to make the actual import activities
 * easier to work with and more effective.
 *
 * @author Scott Fines
 * Date: 11/12/13
 */
class ImportFile {
		private static String IS_DIRECTORY = "isDirectory";
		private static String IS_DIR = "isDir";
		private static Directory directory;
		private FileSystem fileSystem;

		public FileSystem getFileSystem() {
				return fileSystem;
		}

		private interface Directory {
			boolean isDirectory(FileStatus fileStatus) throws IOException;
		}
	
		static {
				try {
				FileStatus.class.getMethod(IS_DIRECTORY, null); 
				directory = new Directory() {
						@Override
						public boolean isDirectory(FileStatus fileStatus) throws IOException {
							try {
								Method method = fileStatus.getClass().getMethod(IS_DIRECTORY, null);
								return (Boolean) method.invoke(fileStatus, null);
							} catch (Exception e) {
								throw new IOException("Error with Hadoop Version, directory lookup off",e);
							}
						}
						
					};
				}
				catch (NoSuchMethodException e) {
					directory = new Directory() {
						@Override
						public boolean isDirectory(FileStatus fileStatus) throws IOException {
							try {
								Method method = fileStatus.getClass().getMethod(IS_DIR, null);
								return (Boolean) method.invoke(fileStatus, null);
							} catch (Exception e) {
								throw new IOException("Error with Hadoop Version, directory lookup off",e);
								
							}
						}
					};
				}			
		}
		
		private static final Logger LOG = Logger.getLogger(ImportFile.class);

		private final String inputPath;
		private List<FileStatus> fileStatus;

		ImportFile(String inputPath) throws IOException {
				this.inputPath = inputPath;
				fileSystem = FileSystem.get(SpliceUtils.config);
		}


		public List<Path> getPaths() throws IOException {
				if(fileStatus==null)
						fileStatus = listStatus(fileSystem,inputPath);

				return Lists.transform(fileStatus,new Function<FileStatus, Path>() {
						@Override
						public Path apply(@Nullable FileStatus fileStatus) {
								//noinspection ConstantConditions
								return fileStatus.getPath();
						}
				});
		}

		/**
		 *
		 * @return the total length of this "file", in bytes
		 * @throws IOException if something goes wrong
		 */
		public long getTotalLength() throws IOException {
				if(fileStatus==null)
						fileStatus = listStatus(fileSystem,inputPath);

				long length=0l;
				for(FileStatus status:fileStatus){
						length+=status.getLen();
				}
				return length;
		}

		private static final PathFilter hiddenFileFilter = new PathFilter(){
				public boolean accept(Path p){
						String name = p.getName();
						return !name.startsWith("_") && !name.startsWith(".");
				}
		};
		/**
		 * Allows for multiple input paths separated by commas
		 * @param input the input path pattern
		 */
		private static Path[] getInputPaths(String input) {
				String [] list = StringUtils.split(input);
				Path[] result = new Path[list.length];
				for (int i = 0; i < list.length; i++) {
						result[i] = new Path(StringUtils.unEscapeString(list[i]));
				}
				return result;
		}

		private static List<FileStatus> listStatus(final FileSystem fs,String input) throws IOException {
				Path[] dirs = getInputPaths(input);
				if (dirs.length == 0)
						throw new IOException("No Path Supplied in job");
				List<Path> errors = Lists.newArrayListWithExpectedSize(0);

				// creates a MultiPathFilter with the hiddenFileFilter and the
				// user provided one (if any).
				List<PathFilter> filters = new ArrayList<PathFilter>();
				filters.add(new PathFilter() {
						@Override
						public boolean accept(Path path) {
								try {
										return fs.getFileStatus(path).isFile();
								} catch (IOException e) {
										throw new RuntimeException(e);
								}
						}
				});
				filters.add(hiddenFileFilter);
				PathFilter inputFilter = new MultiPathFilter(filters);

				List<FileStatus> result = Lists.newArrayListWithExpectedSize(dirs.length);
				for (Path p : dirs) {
						FileStatus[] matches = fs.globStatus(p, inputFilter);
						if (matches == null) {
								errors.add(p);
						} else if (matches.length == 0) {
								errors.add(p);
						} else {
								for (FileStatus globStat : matches) {
										if(!directory.isDirectory(globStat))
												result.add(globStat);
								}
						}
				}

				if (!errors.isEmpty()) {
						throw new FileNotFoundException(errors.toString());
				}
				LOG.info("Total input paths to process : " + result.size());
				return result;
		}

		private static class MultiPathFilter implements PathFilter {
				private List<PathFilter> filters;

				public MultiPathFilter(List<PathFilter> filters) {
						this.filters = filters;
				}

				public boolean accept(Path path) {
						for (PathFilter filter : filters) {
								if (!filter.accept(path)) {
										return false;
								}
						}
						return true;
				}
		}
}
