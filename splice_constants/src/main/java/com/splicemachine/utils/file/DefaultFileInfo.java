package com.splicemachine.utils.file;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 3/13/14
 */
public class DefaultFileInfo implements FileInfo{
		//the default user for when we can't find anything else out
		private static final String DEFAULT_USER = "hbase";
		private static final String DEFAULT_GROUP = "users";
		private final FileSystem fs;

		private volatile String[] userGroupInfo;

		public DefaultFileInfo(FileSystem fs) {
				this.fs = fs;
		}

		private static enum PermissionType{
				OWNER{ @Override public FsAction getAction(FsPermission permission) { return permission.getUserAction(); } },
				GROUP{ @Override public FsAction getAction(FsPermission permission) { return permission.getGroupAction(); } },
				OTHER{ @Override public FsAction getAction(FsPermission permission) { return permission.getOtherAction(); } };

				public FsAction getAction(FsPermission permission){
						throw new UnsupportedOperationException();
				}

				public boolean isWritable(FileStatus status){
						switch (getAction(status.getPermission())){
								case WRITE:
								case WRITE_EXECUTE:
								case READ_WRITE:
								case ALL:
										return true;
								default:
										return false;
						}
				}

				public boolean isReadable(FileStatus status){
						switch(getAction(status.getPermission())){
								case READ:
								case READ_EXECUTE:
								case READ_WRITE:
								case ALL:
										return true;
								default:
										return false;
						}
				}
		}

		@Override
		public boolean isWritable(Path path) throws IOException {
				FileStatus status = fs.getFileStatus(path);
				return getPermissionType(status).isWritable(status);
		}


		@Override
		public boolean isReadable(Path path) throws IOException {
				FileStatus status = fs.getFileStatus(path);
				return getPermissionType(status).isReadable(status);
		}

		@Override
		public boolean isDirectory(Path path) throws IOException {
				return fs.isDirectory(path);
		}

		@Override
		public String[] getUserAndGroup() throws IOException {
				return getUserGroupInformation();
		}

		private PermissionType getPermissionType(FileStatus status) throws IOException {
				String[] currentUserAndGroup = getUserGroupInformation();

				if(currentUserAndGroup[0].equalsIgnoreCase(status.getOwner())){
						return PermissionType.OWNER;
				}else if(currentUserAndGroup[1].equalsIgnoreCase(status.getGroup())){
						return PermissionType.GROUP;
				}else
						return PermissionType.OTHER;
		}


		private String[] getUserGroupInformation() throws IOException {
				if(userGroupInfo==null){
						synchronized (this){
								/*
								 * We need to get the Current user of the system. Since the FileSystem API doesn't allow us
								 * to just pull out that information, we need to get it through a couple of layers of indirection.
								 */
								String user = DEFAULT_USER;
								String group = DEFAULT_GROUP;
								Path homeDirectory = new Path(fs.getConf().get("hbase.rootdir")); // XXX JLEACH TODO Needs to be configurable.
								if(homeDirectory==null){
										userGroupInfo = new String[]{user,group};
										return userGroupInfo;
								}
								FileStatus homeStatus = fs.getFileStatus(homeDirectory);
								String owner = homeStatus.getOwner();
								//owner is probably never null, but just to be safe
								if(owner != null)
										user = owner;

								String g = homeStatus.getGroup();
								if(g!=null)
										group = g;
								userGroupInfo = new String[]{user,group};
						}
				}
				return userGroupInfo;
		}
}
