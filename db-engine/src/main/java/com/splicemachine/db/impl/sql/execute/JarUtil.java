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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedActionException;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FileUtil;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.FileInfoDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.store.access.FileResource;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.io.StorageFile;

public class JarUtil
{
	//
	//State passed in by the caller
    private LanguageConnectionContext lcc;
	private String schemaName;
	private String sqlName;

	//Derived state
	
	private FileResource fr;
	private DataDictionary dd;
	private DataDescriptorGenerator ddg;
	
	//
	//State derived from the caller's context
	private JarUtil(LanguageConnectionContext lcc,
            String schemaName, String sqlName)
		 throws StandardException {
		this.schemaName = schemaName;
		this.sqlName = sqlName;
        this.lcc = lcc;
		fr = lcc.getTransactionExecute().getFileHandler();
		dd = lcc.getDataDictionary();
		ddg = dd.getDataDescriptorGenerator();
	}

	/**
	  install a jar file to the current connection's database.

	  @param schemaName the name for the schema that holds the jar file.
	  @param sqlName the sql name for the jar file.
	  @param externalPath the path for the jar file to add.
	  @return The generationId for the jar file we add.

	  @exception StandardException Opps
	  */
	public static long
	install(LanguageConnectionContext lcc,
            String schemaName, String sqlName, String externalPath)
		 throws StandardException {
		JarUtil jutil = new JarUtil(lcc, schemaName, sqlName);
		InputStream is = null;
		try {
			is = openJarURL(externalPath);
			return jutil.add(is);
		} catch (java.io.IOException fnfe) {
			throw StandardException.newException(SQLState.SQLJ_INVALID_JAR, fnfe, externalPath);
		}
		finally {
			try {if (is != null) is.close();}
			catch (IOException ioe) {}
		}
	}

	/**
	  Add a jar file to the current connection's database.

	  <P> The reason for adding the jar file in this private instance
	  method is that it allows us to share set up logic with drop and
	  replace.
	  @param is A stream for reading the content of the file to add.
	  @exception StandardException Opps
	  */
	private long add(final InputStream is) throws StandardException {
		return lcc.getDatabase().addJar(is,this);
	}

	/**
     * Drop a jar file from the current connection's database.
     * 
     * @param schemaName
     *            the name for the schema that holds the jar file.
     * @param sqlName
     *            the sql name for the jar file.
     * 
     * @exception StandardException
     *                Opps
     */
	public static void
	drop(LanguageConnectionContext lcc, String schemaName, String sqlName)
		 throws StandardException
	{
		JarUtil jutil = new JarUtil(lcc, schemaName,sqlName);
		jutil.drop();
	}

	/**
	  Drop a jar file from the current connection's database.

	  <P> The reason for dropping  the jar file in this private instance
	  method is that it allows us to share set up logic with add and
	  replace.

	  @exception StandardException Opps
	  */
	private void drop() throws StandardException {
		lcc.getDatabase().dropJar(this);
	}

	/**
	  Replace a jar file from the current connection's database with the content of an
	  external file. 


	  @param schemaName the name for the schema that holds the jar file.
	  @param sqlName the sql name for the jar file.
	  @param externalPath the path for the jar file to add.
	  @return The new generationId for the jar file we replace.

	  @exception StandardException Opps
	  */
	public static long
	replace(LanguageConnectionContext lcc, String schemaName, String sqlName,
			String externalPath)
		 throws StandardException
	{
		JarUtil jutil = new JarUtil(lcc, schemaName,sqlName);
		InputStream is = null;
		

		try {
			is = openJarURL(externalPath);

			return jutil.replace(is);
		} catch (java.io.IOException fnfe) {
			throw StandardException.newException(SQLState.SQLJ_INVALID_JAR, fnfe, externalPath);
		}
		finally {
			try {if (is != null) is.close();}
			catch (IOException ioe) {}
		}
	}

	/**
	  Replace a jar file in the current connection's database with the
	  content of an external file.

	  <P> The reason for adding the jar file in this private instance
	  method is that it allows us to share set up logic with add and
	  drop.
	  @param is An input stream for reading the new content of the jar file.
	  @exception StandardException Opps
	  */
	private long replace(InputStream is) throws StandardException {
		return lcc.getDatabase().replaceJar(is,this);
	}

	/**
	  Get the FileInfoDescriptor for the Jar file or null if it does not exist.
	  @exception StandardException Ooops
	  */
	public FileInfoDescriptor getInfo()
		 throws StandardException
	{
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, null, true);
		return dd.getFileInfoDescriptor(sd,sqlName);
	}

	public void notifyLoader(boolean reload) throws StandardException {
		ClassFactory cf = lcc.getLanguageConnectionFactory().getClassFactory();
		cf.notifyModifyJar(reload);
	}

    /**
     * Open an input stream to read a URL or a file.
     * URL is attempted first, if the string does not conform
     * to a URL then an attempt to open it as a regular file
     * is tried.
     * <BR>
     * Attempting the file first can throw a security execption
     * when a valid URL is passed in.
     * The security exception is due to not have the correct permissions
     * to access the bogus file path. To avoid this the order was reversed
     * to attempt the URL first and only attempt a file open if creating
     * the URL throws a MalformedURLException.
     */
    private static InputStream openJarURL(final String externalPath)
        throws IOException
    {
        try {
            return (InputStream) AccessController.doPrivileged
            (new java.security.PrivilegedExceptionAction(){
                
                public Object run() throws IOException {    
                    try {
                        return new URL(externalPath).openStream();
                    } catch (MalformedURLException mfurle)
                    {
                        return new FileInputStream(externalPath);
                    }
                }
            });
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getException();
        }
    }
    
    /**
     * Copy the jar from the externally obtained 
     * input stream into the database
     * @param jarExternalName Name of jar with database structure.
     * @param contents Contents of jar file.
     * @param add true to add, false to replace
     * @param currentGenerationId generation id of existing version, ignored when adding.
     */
    public long setJar(final String jarExternalName,
            final InputStream contents,
            final boolean add,
            final long currentGenerationId)
            throws StandardException {
        try {
            return (Long) AccessController
                    .doPrivileged(new java.security.PrivilegedExceptionAction() {
                        @Override
                        public Object run() throws StandardException {
                            long generationId;
                            if (add)
                                generationId = fr.add(jarExternalName, contents);
                            else
                                generationId = fr.replace(jarExternalName, currentGenerationId, contents);
                            return generationId;
                        }
                    });
        } catch (PrivilegedActionException e) {
            throw (StandardException) e.getException();
        }
    }
    
    /**
      Make an external name for a jar file stored in the database.
      */
    public static String mkExternalName(
            UUID id, 
            String schemaName, 
            String sqlName, 
            char separatorChar) throws StandardException
    {
        return mkExternalNameInternal(
            id, schemaName, sqlName, separatorChar, false, false);
    }

    private static String mkExternalNameInternal(
            UUID id,
            String schemaName,
            String sqlName,
            char separatorChar,
            boolean upgrading,
            boolean newStyle) throws StandardException {
        StringBuilder sb = new StringBuilder(30);
        sb.append(FileResource.JAR_DIRECTORY_NAME);
        sb.append(separatorChar);

        if (!upgrading || newStyle) {
            sb.append(id.toString());
            sb.append(".jar");
        } else {
            sb.append(schemaName);
            sb.append(separatorChar);
            sb.append(sqlName);
            sb.append(".jar");
        }

        return sb.toString();
    }

    /**
     * Upgrade code: upgrade one jar file to new style (>= 10.9)
     *
     * @param tc transaction controller
     * @param fid the jar file to be upgraded
     * @throws StandardException
     */
    public static void upgradeJar(
            TransactionController tc,
            FileInfoDescriptor fid)
            throws StandardException {

        FileResource fh = tc.getFileHandler();

        StorageFile oldFile = fh.getAsFile(
            mkExternalNameInternal(
                fid.getUUID(),
                fid.getSchemaDescriptor().getSchemaName(),
                fid.getName(),
                File.separatorChar,
                true,
                false),
            fid.getGenerationId());

        StorageFile newFile = fh.getAsFile(
            mkExternalNameInternal(
                fid.getUUID(),
                fid.getSchemaDescriptor().getSchemaName(),
                fid.getName(),
                File.separatorChar,
                true,
                true),
            fid.getGenerationId());

        FileUtil.copyFile(
                new File(oldFile.getPath()),
                new File(newFile.getPath()), null);
    }

	public LanguageConnectionContext getLanguageConnectionContext() {
		return lcc;
	}

	public String getSqlName() {
		return sqlName;
	}
	public String getSchemaName() {
		return schemaName;
	}

	public DataDescriptorGenerator getDataDescriptorGenerator() {
		return ddg;
	}

	public FileResource getFileResource() {
		return fr;
	}

}
