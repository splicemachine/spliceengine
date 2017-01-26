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

package com.splicemachine.db.impl.tools.ij;

import java.util.Hashtable;
import java.util.Properties;
import java.io.FileNotFoundException;
import java.io.BufferedInputStream;

import java.io.FileInputStream;
import java.io.IOException;

import com.splicemachine.db.iapi.tools.i18n.*;

/**
 */
public class mtTestCase
{
	public String name = null;
	public String file = null;
	public String propFile = null;
	public float weight = (float).5;
	public Hashtable ignoreErrors = null;
	public String description = null;


	private int iterations;
	private int attempts;

	public void mtTestCase()
	{ };

	public void setName(String name)
	{
		this.name = name;
	}
	public String getName()
	{
		return name;
	}

	public void setFile(String name)
	{
		this.file = name;
	}

	public void setInputDir(String dir)
	{
		file = dir + "/" + file;
	}

	public String getFile()
	{
		return file;
	}
	
	public void setPropFile(String name)
	{
		this.propFile = name;
	}

	public String getPropFile()
	{
		return propFile;
	}

	public void setWeight(int weight)
	{
		this.weight = (float)(weight/100.0);
	}
	
	public void setIgnoreErrors(Hashtable t)
	{
		this.ignoreErrors = t;
	}
	
	public void setDescription(String description)
	{
		this.description = description;
	}

	/**
	** Initialize the test case.  See initialize(String)
	*/
	public synchronized BufferedInputStream initialize() 
			throws FileNotFoundException, IOException
	{
		return initialize(null);
	}

	/**
	** Initizalize the test case.  Loads up the properties
	** file and sets the input stream.  Used to set up
	** prior to running the thread.
	*/
	public synchronized BufferedInputStream initialize(String inputDir) 
			throws FileNotFoundException, IOException
	{
		String filePath; 
		BufferedInputStream	inStream = null;

		// load up properties
		if (propFile != null)
		{	
			BufferedInputStream	propStream;
			Properties		p;
			String propPath = (inputDir == null) ?
						propFile : 
				(inputDir + "/" + propFile);
			
			try 
			{
				propStream = new BufferedInputStream(new FileInputStream(propPath));
			} catch (FileNotFoundException e)
			{
				System.out.println(name+": unable to find properties file "+propPath);
				throw e;
			}

			p = System.getProperties();
			p.load(propStream);
			// for network server need to alter url
			String framework = p.getProperty("framework");
			
			if (framework != null)
            {
                String newURLPrefix = null;
                framework = framework.toUpperCase(java.util.Locale.ENGLISH);
                if (framework.equals("DB2JNET") || framework.equals("DERBYNET"))
                    newURLPrefix= "jdbc:splice:net://localhost:1527/";
                else if (framework.equals("DERBYNETCLIENT"))
                    newURLPrefix = "jdbc:splice://localhost:1527/";
                if (newURLPrefix != null)
                {
                    updateURLProperties(p,newURLPrefix);
                    p.setProperty("ij.user","SPLICE");
                    p.setProperty("ij.password","PWD");
                }
			}
            // this is a special case for the MultiTest.
            // check and alter url if there are any encryption related 
            // properties that need to be set on the url 
            if (("true").equalsIgnoreCase(p.getProperty("encryption"))) 
            {
               String encryptUrl = "dataEncryption=true;bootPassword=Thursday";
               String dbUrl = p.getProperty("ij.database");
               String encryptionAlgorithm = p.getProperty("encryptionAlgorithm");
               if (encryptionAlgorithm != null)
               {
                   p.setProperty(
                       "ij.database",
                       dbUrl + ";" + encryptUrl + ";" + encryptionAlgorithm);
               }
               else
               {
                   p.setProperty("ij.database",dbUrl + ";"+encryptUrl);
               }
            }
            
			// If the initial connection is being specified as a DataSource
			// on the command line using -Dij.dataSource=<dsclassname>
			// then remove the ij.database and ij.protocol property.
            // This is because the ij.database and ij.protocol 
            // will override the ij.dataSource property.
			if (System.getProperty("ij.dataSource") != null)
			{
				p.remove("ij.database");
				p.remove("ij.protocol");
			}
            
			System.setProperties(p);
		}
		// set input stream
		filePath = (inputDir == null) ?
						file : (inputDir + "/" + file);

		try 
		{
			inStream = new BufferedInputStream(new FileInputStream(filePath), 
							utilMain.BUFFEREDFILESIZE);		
		} catch (FileNotFoundException e)
		{
			System.out.println("unable to find properties file "+filePath);
			throw e;
		}
		return inStream;
	}

	/**
	** Attempt to grab this test case.  
	** Uses random number and the weight of this
	** case to determine if the grab was successful.
	** 
	** @return true/false
	*/
	public synchronized boolean grab()
	{
		attempts++;
		if (java.lang.Math.random() < weight)
		{
			iterations++;
			return true;
		}
		else
		{
			return false;
		}
	}

	/**
	** Run the test case.  Invokes IJ to do our
	** dirty work.
	*/
	public void runMe(LocalizedOutput log, LocalizedOutput out, BufferedInputStream infile)
	{
		utilMain	utilInstance;
        LocalizedInput is;
        is = LocalizedResource.getInstance().getNewInput(infile);

		LocalizedInput [] in = { is };
	
		out.println("--------------"+file+"-----------------");
		utilInstance = new utilMain(1, out, ignoreErrors);
		utilInstance.initFromEnvironment();
		utilInstance.setMtUse(true);
		utilInstance.go(in, out);
		log.flush();
		out.flush();
	}

	public void updateURLProperties(Properties p, String newURLPrefix)
	{
		String[] propsToUpdate = {"ij.database", "ij.protocol",
								  "database"};
		for (int i = 0; i < propsToUpdate.length; i++)
		{
			String key = propsToUpdate[i];
			String val = p.getProperty(key);
			if (val != null)
				p.setProperty(key,alterURL(val,newURLPrefix));
		}
	}


	public String alterURL(String url, String newURLPrefix)
	{
        String urlPrefix = "jdbc:splice:";
	
		if (url.startsWith(newURLPrefix))
			return url;

		// If we don't have a URL prefix for this framework
		// just return
		if (newURLPrefix == null)
			return url;
	
		if (url.equals(urlPrefix)) // Replace embedded
			return newURLPrefix;

		if (url.startsWith(urlPrefix))
		{
			//jdbc:splicee jdbc:splice: with our url:
			url = newURLPrefix +
				url.substring(urlPrefix.length());

		}
		else
		{
			if (! (url.startsWith("jdbc:")))
	    {
			url = newURLPrefix + url;
	    }
		}
		//System.out.println("New url:" +url);
		return url;
    }
  

// NOTE: tried invoking ij directly, but had some problems,
// so stick with calling utilMain().	
//	/**
//	** Run the test case.  Invokes IJ to do our
//	** dirty work.
//	*/
//	public void runMe(AppStreamWriter log, AppStreamWriter out, BufferedInputStream infile)
//	{
//		ASCII_UCodeESC_CharStream charStream;
//		ijTokenManager	ijTokMgr;
//		ij	ijParser;
//	
//		
//		out.println("--------------"+file+"-----------------");
//		charStream = new ASCII_UCodeESC_CharStream(in, 1, 1);
//		ijTokMgr = new ijTokenManager(charStream);
//		ijParser = new ij(ijTokMgr, System.out, this);
//		log.flush();
//		out.flush();
//	}

	/**
	** Name says it all
	*/
	public String toString()
	{
		return "name: "+name+
				"\n\tfile: "+file+
				"\n\tproperties: "+propFile+
				"\n\tweight: "+weight+
				"\n\tignoreErrors: "+ignoreErrors+
				"\n\tdescription: "+description;
	}

	
}
