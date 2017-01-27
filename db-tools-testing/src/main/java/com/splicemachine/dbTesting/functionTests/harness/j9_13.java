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

package com.splicemachine.dbTesting.functionTests.harness;

import java.util.Vector;
import java.util.StringTokenizer;
import java.util.Properties;


/**
  <p>This class is for IBM's J9 jdk 1.3.

 */
public class j9_13 extends jvm {

	public String getName(){return "j9_13";}
    public j9_13(boolean noasyncgc, boolean verbosegc, boolean noclassgc,
    long ss, long oss, long ms, long mx, String classpath, String prof,
    boolean verify, boolean noverify, boolean nojit, Vector D) {
        super(noasyncgc,verbosegc,noclassgc,ss,oss,ms,mx,classpath,prof,
		verify,noverify,nojit,D);
    }
    // more typical use:
    public j9_13(String classpath, Vector D) {
        super(classpath,D);
    }
    // more typical use:
    public j9_13(long ms, long mx, String classpath, Vector D) {
        super(ms,mx,classpath,D);
    }
    // actual use
    public j9_13() {
	Properties sp = System.getProperties();
	String srvJvm = sp.getProperty("serverJvm");
	if ((srvJvm!=null) && ((srvJvm.toUpperCase().startsWith("J9")) || (srvJvm.equalsIgnoreCase("wsdd5.6"))))
	{
		String wshome = guessWSHome();
		// note, may have to switch to sep instead of hardcoding the slashes...
		setJavaCmd(wshome+"/wsdd5.6/ive/bin/j9");
	}
	else
		setJavaCmd("j9");
    }

    // return the command line to invoke this VM.  The caller then adds
    // the class and program arguments.
    public Vector getCommandLine() 
    {

        StringBuffer sb = new StringBuffer();
        Vector v = super.getCommandLine();

        appendOtherFlags(sb);
        String s = sb.toString();
        StringTokenizer st = new StringTokenizer(s);
        while (st.hasMoreTokens())
        {
            v.addElement(st.nextToken());
        }
        return v;
	}

	public void appendOtherFlags(StringBuffer sb)
	{

	Properties sp = System.getProperties();
	String bootcp = sp.getProperty("bootcp");
	String srvJvm = sp.getProperty("serverJvm");
	// if we're coming in to be the server jvm for networkserver testing on j9,
	// bootcp is null, so we need to try to setup the bootclasspath from scratch
	// for now, assume we're only interested in doing this for wsdd5.6, worry about
	// newer versions, multiple class libraries, or multiple releases later.
	if ((srvJvm !=null ) && ((srvJvm.toUpperCase().startsWith("J9")) || (srvJvm.equalsIgnoreCase("wsdd5.6"))))
	{
		String pathsep = System.getProperty("path.separator");
		String wshome = guessWSHome();
		// note, may have to switch to sep instead of hardcoding the slashes...
		sb.append(" -Xbootclasspath/a:" + wshome + "/wsdd5.6/ive/lib/jclMax/classes.zip"
			+ pathsep + wshome + "/wsdd5.6/ive/lib/charconv.zip"
			+ pathsep + wshome + "/wsdd5.6/ive/lib/jclMax");
	} 
	else
		sb.append(" -Xbootclasspath/a:" + bootcp);
        if (noasyncgc) warn("j9_13 does not support noasyncgc");
        if (verbosegc) sb.append(" -verbose:gc");
        if (noclassgc) warn("j9_13 does not support noclassgc");
        if (ss>=0) warn("j9_13 does not support ss");
        if (oss>=0) warn("j9_13 does not support oss");
        if (ms>=0) {
          sb.append(" -Xss");
          sb.append(ms);
		  //sb.append("k");
        }
        if (mx>=0) {
          sb.append(" -Xmx");
          sb.append(mx);
		  //sb.append("k");
        }
        if (classpath!=null) warn("j9_13 does not support classpath, use -Xbootclasspath,-Xbootclasspath/p,-Xbootclasspath/a"); 
        if (prof!=null) warn("j9_13 does not support prof");
        if (verify) sb.append(" -verify");
        if (noverify) warn("j9_13 does not support noverify");
        if (nojit) sb.append(" -Xnojit");
        if (D != null)
          for (int i=0; i<D.size();i++) {
	        sb.append(" -D");
	        sb.append((String)(D.elementAt(i)));
          }
    }
	public String getDintro() { return "-D"; }

	protected void setSecurityProps()
	{
		System.out.println("Note: J9 tests do not run with security manager");		
	}
}
