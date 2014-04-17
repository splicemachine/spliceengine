package com.splicemachine.derby.impl.load;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
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
						//noinspection NullArgumentToVariableArgMethod
						FileStatus.class.getMethod(IS_DIRECTORY, null);
				directory = new Directory() {
						@Override
						@SuppressWarnings("NullArgumentToVariableArgMethod")
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
						@SuppressWarnings("NullArgumentToVariableArgMethod")
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
		 * Allows for multiple input paths separated by spaces
		 * @param input the input path pattern (BROKEN FOR THE MOMENT)
		 */
		private static ArrayList<Path> getInputPaths(String input) {
				String [] list = StringUtils.split(input);
				ArrayList<Path> result = new ArrayList<Path>();
				for (String aList : list) {
						result.add(new Path(StringUtils.unEscapeString(aList)));
				}
				return result;
		}

		/** 
		 * We want import to emulate the Path API as much as possible (TODO)
		 * In the meantime it does the following 
		 *   - handles importing a single file
		 *   - handles importing a directory of files
		 *   - if a directory is provided, processes all subdirectories recursively
		 *   - (still needs to handle filtering properly - * chars, etc) 
		 * @param fs The File system to use
		 * @param input - string of files to import - as a file or a directory.
		 * @return - a list of files to import
		 * @throws IOException
		 */
		
		private static List<FileStatus> listStatus(final FileSystem fs,String input) throws IOException {
				ArrayList<Path> dirsOrFiles = getInputPaths(input);
				if (dirsOrFiles.size() == 0)
						throw new IOException("No Path Or File Supplied in job");
				List<Path> errors = Lists.newArrayListWithExpectedSize(0);

				// creates a MultiPathFilter with the hiddenFileFilter and the
				// user provided one (if any).
				List<PathFilter> filters = new ArrayList<PathFilter>();
				filters.add(hiddenFileFilter);
				PathFilter inputFilter = new MultiPathFilter(filters);
				
				List<FileStatus> result = Lists.newArrayListWithExpectedSize(dirsOrFiles.size());
				while (dirsOrFiles.size() > 0) {
					Path p = dirsOrFiles.remove(0);  // remove item from "queue"
					FileStatus[] matches = fs.globStatus(p, inputFilter);
					if (matches == null) {  // any error should be collected
						errors.add(p);
					} else if (matches.length == 0) {
						errors.add(p);
					} else {
						for (FileStatus globStat : matches) {
								if(!directory.isDirectory(globStat))
										result.add(globStat);  // add file
								else { // process results of directory and add to the list
									FileStatus[] subdirMatches = fs.listStatus(globStat.getPath(), inputFilter);
									for (FileStatus match : subdirMatches) {
										dirsOrFiles.add(match.getPath());
									}
								}
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
