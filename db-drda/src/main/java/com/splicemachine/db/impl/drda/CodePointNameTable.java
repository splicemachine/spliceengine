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
package com.splicemachine.db.impl.drda;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This class has a hashtable of CodePoint values.  It is used by the tracing
 * code and by the protocol testing code
 * It is arranged in alphabetical order.
 */

public class CodePointNameTable{
    private static final Map<Integer, String> codePointToName;
    private static final Map<String, Integer> nameToCodePoint;

    private CodePointNameTable(){
        // Cannot Instantiate
    }

    static{
        Map<Integer, String> cpTN=new HashMap<>();
        cpTN.put(CodePoint.ABNUOWRM,"ABNUOWRM");
        cpTN.put(CodePoint.ACCRDB,"ACCRDB");
        cpTN.put(CodePoint.ACCRDBRM,"ACCRDBRM");
        cpTN.put(CodePoint.ACCSEC,"ACCSEC");
        cpTN.put(CodePoint.ACCSECRD,"ACCSECRD");
        cpTN.put(CodePoint.AGENT,"AGENT");
        cpTN.put(CodePoint.AGNPRMRM,"AGNPRMRM");
        cpTN.put(CodePoint.BGNBND,"BGNBND");
        cpTN.put(CodePoint.BGNBNDRM,"BGNBNDRM");
        cpTN.put(CodePoint.BNDSQLSTT,"BNDSQLSTT");
        cpTN.put(CodePoint.CCSIDSBC,"CCSIDSBC");
        cpTN.put(CodePoint.CCSIDMBC,"CCSIDMBC");
        cpTN.put(CodePoint.CCSIDDBC,"CCSIDDBC");
        cpTN.put(CodePoint.CLSQRY,"CLSQRY");
        cpTN.put(CodePoint.CMDATHRM,"CMDATHRM");
        cpTN.put(CodePoint.CMDCHKRM,"CMDCHKRM");
        cpTN.put(CodePoint.CMDCMPRM,"CMDCMPRM");
        cpTN.put(CodePoint.CMDNSPRM,"CMDNSPRM");
        cpTN.put(CodePoint.CMMRQSRM,"CMMRQSRM");
        cpTN.put(CodePoint.CMDVLTRM,"CMDVLTRM");
        cpTN.put(CodePoint.CNTQRY,"CNTQRY");
        cpTN.put(CodePoint.CRRTKN,"CRRTKN");
        cpTN.put(CodePoint.DRPPKG,"DRPPKG");
        cpTN.put(CodePoint.DSCRDBTBL,"DSCRDBTBL");
        cpTN.put(CodePoint.DSCINVRM,"DSCINVRM");
        cpTN.put(CodePoint.DSCSQLSTT,"DSCSQLSTT");
        cpTN.put(CodePoint.DTAMCHRM,"DTAMCHRM");
        cpTN.put(CodePoint.ENDBND,"ENDBND");
        cpTN.put(CodePoint.ENDQRYRM,"ENDQRYRM");
        cpTN.put(CodePoint.ENDUOWRM,"ENDUOWRM");
        cpTN.put(CodePoint.EXCSAT,"EXCSAT");
        cpTN.put(CodePoint.EXCSATRD,"EXCSATRD");
        cpTN.put(CodePoint.EXCSQLIMM,"EXCSQLIMM");
        cpTN.put(CodePoint.EXCSQLSET,"EXCSQLSET");
        cpTN.put(CodePoint.EXCSQLSTT,"EXCSQLSTT");
        cpTN.put(CodePoint.EXTNAM,"EXTNAM");
        cpTN.put(CodePoint.FRCFIXROW,"FRCFIXROW");
        cpTN.put(CodePoint.MAXBLKEXT,"MAXBLKEXT");
        cpTN.put(CodePoint.MAXRSLCNT,"MAXRSLCNT");
        cpTN.put(CodePoint.MGRDEPRM,"MGRDEPRM");
        cpTN.put(CodePoint.MGRLVLLS,"MGRLVLLS");
        cpTN.put(CodePoint.MGRLVLRM,"MGRLVLRM");
        cpTN.put(CodePoint.MONITOR,"MONITOR");
        cpTN.put(CodePoint.NBRROW,"NBRROW");
        cpTN.put(CodePoint.OBJNSPRM,"OBJNSPRM");
        cpTN.put(CodePoint.OPNQFLRM,"OPNQFLRM");
        cpTN.put(CodePoint.OPNQRY,"OPNQRY");
        cpTN.put(CodePoint.OPNQRYRM,"OPNQRYRM");
        cpTN.put(CodePoint.OUTEXP,"OUTEXP");
        cpTN.put(CodePoint.OUTOVR,"OUTOVR");
        cpTN.put(CodePoint.OUTOVROPT,"OUTOVROPT");
        cpTN.put(CodePoint.PASSWORD,"PASSWORD");
        cpTN.put(CodePoint.PKGID,"PKGID");
        cpTN.put(CodePoint.PKGBNARM,"PKGBNARM");
        cpTN.put(CodePoint.PKGBPARM,"PKGBPARM");
        cpTN.put(CodePoint.PKGNAMCSN,"PKGNAMCSN");
        cpTN.put(CodePoint.PKGNAMCT,"PKGNAMCT");
        cpTN.put(CodePoint.PRCCNVRM,"PRCCNVRM");
        cpTN.put(CodePoint.PRDID,"PRDID");
        cpTN.put(CodePoint.PRDDTA,"PRDDTA");
        cpTN.put(CodePoint.PRMNSPRM,"PRMNSPRM");
        cpTN.put(CodePoint.PRPSQLSTT,"PRPSQLSTT");
        cpTN.put(CodePoint.QRYBLKCTL,"QRYBLKCTL");
        cpTN.put(CodePoint.QRYBLKRST,"QRYBLKRST");
        cpTN.put(CodePoint.QRYBLKSZ,"QRYBLKSZ");
        cpTN.put(CodePoint.QRYCLSIMP,"QRYCLSIMP");
        cpTN.put(CodePoint.QRYCLSRLS,"QRYCLSRLS");
        cpTN.put(CodePoint.QRYDSC,"QRYDSC");
        cpTN.put(CodePoint.QRYDTA,"QRYDTA");
        cpTN.put(CodePoint.QRYINSID,"QRYINSID");
        cpTN.put(CodePoint.QRYNOPRM,"QRYNOPRM");
        cpTN.put(CodePoint.QRYPOPRM,"QRYPOPRM");
        cpTN.put(CodePoint.QRYRELSCR,"QRYRELSCR");
        cpTN.put(CodePoint.QRYRFRTBL,"QRYRFRTBL");
        cpTN.put(CodePoint.QRYROWNBR,"QRYROWNBR");
        cpTN.put(CodePoint.QRYROWSNS,"QRYROWSNS");
        cpTN.put(CodePoint.QRYRTNDTA,"QRYRTNDTA");
        cpTN.put(CodePoint.QRYSCRORN,"QRYSCRORN");
        cpTN.put(CodePoint.QRYROWSET,"QRYROWSET");
        cpTN.put(CodePoint.RDBAFLRM,"RDBAFLRM");
        cpTN.put(CodePoint.RDBACCCL,"RDBACCCL");
        cpTN.put(CodePoint.RDBACCRM,"RDBACCRM");
        cpTN.put(CodePoint.RDBALWUPD,"RDBALWUPD");
        cpTN.put(CodePoint.RDBATHRM,"RDBATHRM");
        cpTN.put(CodePoint.RDBCMM,"RDBCMM");
        cpTN.put(CodePoint.RDBCMTOK,"RDBCMTOK");
        cpTN.put(CodePoint.RDBNACRM,"RDBNACRM");
        cpTN.put(CodePoint.RDBNAM,"RDBNAM");
        cpTN.put(CodePoint.RDBNFNRM,"RDBNFNRM");
        cpTN.put(CodePoint.RDBRLLBCK,"RDBRLLBCK");
        cpTN.put(CodePoint.RDBUPDRM,"RDBUPDRM");
        cpTN.put(CodePoint.REBIND,"REBIND");
        cpTN.put(CodePoint.RSCLMTRM,"RSCLMTRM");
        cpTN.put(CodePoint.RSLSETRM,"RSLSETRM");
        cpTN.put(CodePoint.RTNEXTDTA,"RTNEXTDTA");
        cpTN.put(CodePoint.RTNSQLDA,"RTNSQLDA");
        cpTN.put(CodePoint.SECCHK,"SECCHK");
        cpTN.put(CodePoint.SECCHKCD,"SECCHKCD");
        cpTN.put(CodePoint.SECCHKRM,"SECCHKRM");
        cpTN.put(CodePoint.SECMEC,"SECMEC");
        cpTN.put(CodePoint.SECMGRNM,"SECMGRNM");
        cpTN.put(CodePoint.SECTKN,"SECTKN");
        cpTN.put(CodePoint.SPVNAM,"SPVNAM");
        cpTN.put(CodePoint.SQLAM,"SQLAM");
        cpTN.put(CodePoint.SQLATTR,"SQLATTR");
        cpTN.put(CodePoint.SQLCARD,"SQLCARD");
        cpTN.put(CodePoint.SQLERRRM,"SQLERRRM");
        cpTN.put(CodePoint.SQLDARD,"SQLDARD");
        cpTN.put(CodePoint.SQLDTA,"SQLDTA");
        cpTN.put(CodePoint.SQLDTARD,"SQLDTARD");
        cpTN.put(CodePoint.SQLSTT,"SQLSTT");
        cpTN.put(CodePoint.SQLSTTVRB,"SQLSTTVRB");
        cpTN.put(CodePoint.SRVCLSNM,"SRVCLSNM");
        cpTN.put(CodePoint.SRVRLSLV,"SRVRLSLV");
        cpTN.put(CodePoint.SRVNAM,"SRVNAM");
        cpTN.put(CodePoint.SVRCOD,"SVRCOD");
        cpTN.put(CodePoint.SYNCCTL,"SYNCCTL");
        cpTN.put(CodePoint.SYNCLOG,"SYNCLOG");
        cpTN.put(CodePoint.SYNCRSY,"SYNCRSY");
        cpTN.put(CodePoint.SYNTAXRM,"SYNTAXRM");
        cpTN.put(CodePoint.TRGNSPRM,"TRGNSPRM");
        cpTN.put(CodePoint.TYPDEFNAM,"TYPDEFNAM");
        cpTN.put(CodePoint.TYPDEFOVR,"TYPDEFOVR");
        cpTN.put(CodePoint.TYPSQLDA,"TYPSQLDA");
        cpTN.put(CodePoint.UOWDSP,"UOWDSP");
        cpTN.put(CodePoint.USRID,"USRID");
        cpTN.put(CodePoint.VALNSPRM,"VALNSPRM");
        cpTN.put(CodePoint.PBSD,"PBSD");
        cpTN.put(CodePoint.PBSD_ISO,"PBSD_ISO");
        cpTN.put(CodePoint.PBSD_SCHEMA,"PBSD_SCHEMA");
        cpTN.put(CodePoint.UNICODEMGR,"UNICODEMGR");

        codePointToName=Collections.unmodifiableMap(cpTN);
        Map<String,Integer> nTCP = new HashMap<>(cpTN.size());
        for(Integer key : cpTN.keySet()){
            nTCP.put(cpTN.get(key),key);
        }
        nameToCodePoint = Collections.unmodifiableMap(nTCP);
    }

    public static String lookup(int codePoint){
        return codePointToName.get(codePoint);
    }

    public static Integer lookup(String name){
        return nameToCodePoint.get(name);
    }

}
