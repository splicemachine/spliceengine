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


public class jdk16 extends jvm {
    
    public String getName(){return "jdk16";}
    public jdk16(boolean noasyncgc, boolean verbosegc, boolean noclassgc,
        long ss, long oss, long ms, long mx, String classpath, String prof,
        boolean verify, boolean noverify, boolean nojit, Vector D) {
        super(noasyncgc,verbosegc,noclassgc,ss,oss,ms,mx,classpath,prof,
            verify,noverify,nojit,D);
    }
    
    public jdk16(String classpath, Vector D) {
        super(classpath,D);
    }
    
    public jdk16(long ms, long mx, String classpath, Vector D) {
        super(ms,mx,classpath,D);
    }
    
    public jdk16() { }
    
    
    public Vector getCommandLine() {
        StringBuffer sb = new StringBuffer();
        Vector v = super.getCommandLine();
        appendOtherFlags(sb);
        String s = sb.toString();
        StringTokenizer st = new StringTokenizer(s);
        while (st.hasMoreTokens()) {
            v.addElement(st.nextToken());
        }
        return v;
    }
    
    public void appendOtherFlags(StringBuffer sb) {
        if (noasyncgc) warn("jdk16 does not support noasyncgc");
        if (verbosegc) sb.append(" -verbose:gc");
        if (noclassgc) sb.append(" -Xnoclassgc");
        if (ss>=0) warn("jdk16 does not support ss");
        if (oss>=0) warn("jdk16 does not support oss");
        if (ms>=0) {
            sb.append(" -ms");
            sb.append(ms);
        }
        if (mx>=0) {
            sb.append(" -mx");
            sb.append(mx);
        }
        if (classpath!=null) {
            sb.append(" -classpath ");
            sb.append(classpath);
        }
        if (prof!=null) warn("jdk16 does not support prof");
        if (verify) warn("jdk16 does not support verify");
        if (noverify) warn("jdk16 does not support noverify");
        if (nojit) sb.append(" -Djava.compiler=NONE");
        if (D != null)
            for (int i=0; i<D.size();i++) {
            sb.append(" -D");
            sb.append((String)(D.elementAt(i)));
            }
    }
    public String getDintro() { return "-D"; }
}
