/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.services.monitor;

import com.splicemachine.db.iapi.reference.Property;

import com.splicemachine.db.iapi.services.io.FileUtil;
import com.splicemachine.db.iapi.services.info.ProductVersionHolder;
import com.splicemachine.db.iapi.services.info.ProductGenusNames;

import java.io.FileInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import java.util.Properties;

/**
	Implementation of the monitor that uses the class loader
	that the its was loaded in for all class loading.

*/

public final class FileMonitor extends BaseMonitor
{

	/* Fields */
	private File home;

	private ProductVersionHolder engineVersion;

	public FileMonitor() {
		initialize(true);
		applicationProperties = readApplicationProperties();
	}

	public FileMonitor(java.util.Properties properties, java.io.PrintStream log) {
		runWithState(properties, log);
	}



	private InputStream PBapplicationPropertiesStream()
	  throws IOException {

		File sr = FileUtil.newFile(home, Property.PROPERTIES_FILE);

		if (!sr.exists())
			return null;

		return new FileInputStream(sr);
	}

	public Object getEnvironment() {
		return home;
	}

    /**
     * Create a ThreadGroup and set the daemon property to make sure
     * the group is destroyed and garbage collected when all its
     * members have finished (i.e., either when the driver is
     * unloaded, or when the last database is shut down).
     */
    private static ThreadGroup createDaemonGroup() {
        try {
            ThreadGroup group = new ThreadGroup("derby.daemons");
            group.setDaemon(true);
            return group;
        } catch (SecurityException se) {
            // In case of a lacking privilege, silently return null and let
            // the daemon threads be created in the default thread group.
            return null;
        }
    }

	/**
		SECURITY WARNING.

		This method is run in a privileged block in a Java 2 environment.

		Set the system home directory.  Returns false if it couldn't for
		some reason.

	**/
	private boolean PBinitialize(boolean lite)
	{
		if (!lite) {
            daemonGroup = createDaemonGroup();
		}

		InputStream versionStream = getClass().getResourceAsStream(ProductGenusNames.DBMS_INFO);

		engineVersion = ProductVersionHolder.getProductVersionHolderFromMyEnv(versionStream);

		String systemHome;
		// create the system home directory if it doesn't exist
		try {
			// SECURITY PERMISSION - OP2
			systemHome = System.getProperty(Property.SYSTEM_HOME_PROPERTY);
		} catch (SecurityException se) {
			// system home will be the current directory
			systemHome = null;
		}

		if (systemHome != null) {
			home = new File(systemHome);

			// SECURITY PERMISSION - OP2a
			if (home.exists()) {
				if (!home.isDirectory()) {
					report(Property.SYSTEM_HOME_PROPERTY + "=" + systemHome
						+ " does not represent a directory");
					return false;
				}
			} else if (!lite) {

                boolean created = false;
				try {
					// SECURITY PERMISSION - OP2b
                    // Attempt to create just the folder initially
                    // which does not require read permission on
                    // the parent folder. This is to allow a policy
                    // file to limit file permissions for db.jar
                    // to be contained under db.system.home.
                    // If the folder cannot be created that way
                    // due to missing parent folder(s) 
                    // then mkdir() will return false and thus
                    // mkdirs will be called to create the
                    // intermediate folders. This use of mkdir()
                    // and mkdirs() retains existing (pre10.3) behaviour
                    // but avoids requiring read permission on the parent
                    // directory if it exists.
                    created = home.mkdir() || home.mkdirs();
				} catch (SecurityException se) {
					return false;
				}

                if (created) {
                    FileUtil.limitAccessToOwner(home);
                }
			}
		}

		return true;
	}

	/**
		SECURITY WARNING.

		This method is run in a privileged block in a Java 2 environment.

		Return a property from the JVM's system set.
		In a Java2 environment this will be executed as a privileged block
		if and only if the property starts with 'db.'.
		If a SecurityException occurs, null is returned.
	*/
	private String PBgetJVMProperty(String key) {

		try {
			// SECURITY PERMISSION - OP1
			return System.getProperty(key);
		} catch (SecurityException se) {
			return null;
		}
	}

	/*
	** Priv block code, moved out of the old Java2 version.
	*/

	/**
		Initialize the system in a privileged block.
	**/
	final boolean initialize(final boolean lite)
	{
        // SECURITY PERMISSION - OP2, OP2a, OP2b
        return (Boolean) AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                return PBinitialize(lite);
            }
        });
	}

	final Properties getDefaultModuleProperties() {
        // SECURITY PERMISSION - IP1
        return (Properties) AccessController.doPrivileged(
                new PrivilegedAction() {
            public Object run() {
                return FileMonitor.super.getDefaultModuleProperties();
            }
        });
    }

	public final String getJVMProperty(final String key) {
		if (!key.startsWith("derby."))
			return PBgetJVMProperty(key);

        // SECURITY PERMISSION - OP1
        return (String) AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                return PBgetJVMProperty(key);
            }
        });
	}

	public synchronized final Thread getDaemonThread(
            final Runnable task,
            final String name,
            final boolean setMinPriority) {
        return (Thread) AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                try {
                    return FileMonitor.super.getDaemonThread(
                            task, name, setMinPriority);
                } catch (IllegalThreadStateException e) {
                    // We may get an IllegalThreadStateException if all the
                    // previously running daemon threads have completed and the
                    // daemon group has been automatically destroyed. If that's
                    // what happened, create a new daemon group and try again.
                    if (daemonGroup != null && daemonGroup.isDestroyed()) {
                        daemonGroup = createDaemonGroup();
                        return FileMonitor.super.getDaemonThread(
                                task, name, setMinPriority);
                    } else {
                        throw e;
                    }
                }
            }
        });
    }

	public final void setThreadPriority(final int priority) {
        AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                FileMonitor.super.setThreadPriority(priority);
                return null;
            }
        });
	}

	final InputStream applicationPropertiesStream()
	  throws IOException {
		try {
			// SECURITY PERMISSION - OP3
			return (InputStream) AccessController.doPrivileged(
                    new PrivilegedExceptionAction() {
                public Object run() throws IOException {
                    return PBapplicationPropertiesStream();
                }
            });
		}
        catch (java.security.PrivilegedActionException pae)
        {
			throw (IOException) pae.getException();
		}
	}

	public final ProductVersionHolder getEngineVersion() {
		return engineVersion;
	}
}
