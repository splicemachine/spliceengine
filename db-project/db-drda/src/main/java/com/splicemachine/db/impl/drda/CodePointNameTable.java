/*

   Derby - Class com.splicemachine.db.impl.drda.CodePointNameTable

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package com.splicemachine.db.impl.drda;

import java.util.concurrent.ConcurrentHashMap;

/**
  This class has a hashtable of CodePoint values.  It is used by the tracing
  code and by the protocol testing code
  It is arranged in alphabetical order.
*/

public class CodePointNameTable {
     public static ConcurrentHashMap<Integer,String> codePointToName = new ConcurrentHashMap<Integer, String>();
     public static ConcurrentHashMap<String,Integer> nameToCodePoint = new ConcurrentHashMap<String, Integer>();

    private CodePointNameTable() {
        // Cannot Instantiate
    }

    static {
         codePointToName.put(new Integer(CodePoint.ABNUOWRM), "ABNUOWRM");
         codePointToName.put(new Integer(CodePoint.ACCRDB), "ACCRDB");
         codePointToName.put(new Integer(CodePoint.ACCRDBRM), "ACCRDBRM");
         codePointToName.put(new Integer(CodePoint.ACCSEC), "ACCSEC");
         codePointToName.put(new Integer(CodePoint.ACCSECRD), "ACCSECRD");
         codePointToName.put(new Integer(CodePoint.AGENT), "AGENT");
         codePointToName.put(new Integer(CodePoint.AGNPRMRM), "AGNPRMRM");
         codePointToName.put(new Integer(CodePoint.BGNBND), "BGNBND");
         codePointToName.put(new Integer(CodePoint.BGNBNDRM), "BGNBNDRM");
         codePointToName.put(new Integer(CodePoint.BNDSQLSTT), "BNDSQLSTT");
         codePointToName.put(new Integer(CodePoint.CCSIDSBC), "CCSIDSBC");
         codePointToName.put(new Integer(CodePoint.CCSIDMBC), "CCSIDMBC");
         codePointToName.put(new Integer(CodePoint.CCSIDDBC), "CCSIDDBC");
         codePointToName.put(new Integer(CodePoint.CLSQRY), "CLSQRY");
         codePointToName.put(new Integer(CodePoint.CMDATHRM), "CMDATHRM");
         codePointToName.put(new Integer(CodePoint.CMDCHKRM), "CMDCHKRM");
         codePointToName.put(new Integer(CodePoint.CMDCMPRM), "CMDCMPRM");
         codePointToName.put(new Integer(CodePoint.CMDNSPRM), "CMDNSPRM");
         codePointToName.put(new Integer(CodePoint.CMMRQSRM), "CMMRQSRM");
         codePointToName.put(new Integer(CodePoint.CMDVLTRM), "CMDVLTRM");
         codePointToName.put(new Integer(CodePoint.CNTQRY), "CNTQRY");
         codePointToName.put(new Integer(CodePoint.CRRTKN), "CRRTKN");
         codePointToName.put(new Integer(CodePoint.DRPPKG), "DRPPKG");
         codePointToName.put(new Integer(CodePoint.DSCRDBTBL), "DSCRDBTBL");
         codePointToName.put(new Integer(CodePoint.DSCINVRM), "DSCINVRM");
         codePointToName.put(new Integer(CodePoint.DSCSQLSTT), "DSCSQLSTT");
         codePointToName.put(new Integer(CodePoint.DTAMCHRM), "DTAMCHRM");
         codePointToName.put(new Integer(CodePoint.ENDBND), "ENDBND");
         codePointToName.put(new Integer(CodePoint.ENDQRYRM), "ENDQRYRM");
         codePointToName.put(new Integer(CodePoint.ENDUOWRM), "ENDUOWRM");
         codePointToName.put(new Integer(CodePoint.EXCSAT), "EXCSAT");
         codePointToName.put(new Integer(CodePoint.EXCSATRD), "EXCSATRD");
         codePointToName.put(new Integer(CodePoint.EXCSQLIMM), "EXCSQLIMM");
         codePointToName.put(new Integer(CodePoint.EXCSQLSET), "EXCSQLSET");
         codePointToName.put(new Integer(CodePoint.EXCSQLSTT), "EXCSQLSTT");
         codePointToName.put(new Integer(CodePoint.EXTNAM), "EXTNAM");
         codePointToName.put(new Integer(CodePoint.FRCFIXROW), "FRCFIXROW");
         codePointToName.put(new Integer(CodePoint.MAXBLKEXT), "MAXBLKEXT");
         codePointToName.put(new Integer(CodePoint.MAXRSLCNT), "MAXRSLCNT");
         codePointToName.put(new Integer(CodePoint.MGRDEPRM), "MGRDEPRM");
         codePointToName.put(new Integer(CodePoint.MGRLVLLS), "MGRLVLLS");
         codePointToName.put(new Integer(CodePoint.MGRLVLRM), "MGRLVLRM");
         codePointToName.put(new Integer(CodePoint.MONITOR), "MONITOR");
         codePointToName.put(new Integer(CodePoint.NBRROW), "NBRROW");
         codePointToName.put(new Integer(CodePoint.OBJNSPRM), "OBJNSPRM");
         codePointToName.put(new Integer(CodePoint.OPNQFLRM), "OPNQFLRM");
         codePointToName.put(new Integer(CodePoint.OPNQRY), "OPNQRY");
         codePointToName.put(new Integer(CodePoint.OPNQRYRM), "OPNQRYRM");
         codePointToName.put(new Integer(CodePoint.OUTEXP), "OUTEXP");
         codePointToName.put(new Integer(CodePoint.OUTOVR), "OUTOVR");
         codePointToName.put(new Integer(CodePoint.OUTOVROPT), "OUTOVROPT");
         codePointToName.put(new Integer(CodePoint.PASSWORD), "PASSWORD");
         codePointToName.put(new Integer(CodePoint.PKGID), "PKGID");
         codePointToName.put(new Integer(CodePoint.PKGBNARM), "PKGBNARM");
         codePointToName.put(new Integer(CodePoint.PKGBPARM), "PKGBPARM");
         codePointToName.put(new Integer(CodePoint.PKGNAMCSN), "PKGNAMCSN");
         codePointToName.put(new Integer(CodePoint.PKGNAMCT), "PKGNAMCT");
         codePointToName.put(new Integer(CodePoint.PRCCNVRM), "PRCCNVRM");
         codePointToName.put(new Integer(CodePoint.PRDID), "PRDID");
         codePointToName.put(new Integer(CodePoint.PRDDTA), "PRDDTA");
         codePointToName.put(new Integer(CodePoint.PRMNSPRM), "PRMNSPRM");
         codePointToName.put(new Integer(CodePoint.PRPSQLSTT), "PRPSQLSTT");
         codePointToName.put(new Integer(CodePoint.QRYBLKCTL), "QRYBLKCTL");
         codePointToName.put(new Integer(CodePoint.QRYBLKRST), "QRYBLKRST");
         codePointToName.put(new Integer(CodePoint.QRYBLKSZ), "QRYBLKSZ");
         codePointToName.put(new Integer(CodePoint.QRYCLSIMP), "QRYCLSIMP");
         codePointToName.put(new Integer(CodePoint.QRYCLSRLS), "QRYCLSRLS");
         codePointToName.put(new Integer(CodePoint.QRYDSC), "QRYDSC");
         codePointToName.put(new Integer(CodePoint.QRYDTA), "QRYDTA");
         codePointToName.put(new Integer(CodePoint.QRYINSID), "QRYINSID");
         codePointToName.put(new Integer(CodePoint.QRYNOPRM), "QRYNOPRM");
         codePointToName.put(new Integer(CodePoint.QRYPOPRM), "QRYPOPRM");
         codePointToName.put(new Integer(CodePoint.QRYRELSCR), "QRYRELSCR");
         codePointToName.put(new Integer(CodePoint.QRYRFRTBL), "QRYRFRTBL");
         codePointToName.put(new Integer(CodePoint.QRYROWNBR), "QRYROWNBR");
         codePointToName.put(new Integer(CodePoint.QRYROWSNS), "QRYROWSNS");
         codePointToName.put(new Integer(CodePoint.QRYRTNDTA), "QRYRTNDTA");
         codePointToName.put(new Integer(CodePoint.QRYSCRORN), "QRYSCRORN");
         codePointToName.put(new Integer(CodePoint.QRYROWSET), "QRYROWSET");
         codePointToName.put(new Integer(CodePoint.RDBAFLRM), "RDBAFLRM");
         codePointToName.put(new Integer(CodePoint.RDBACCCL), "RDBACCCL");
         codePointToName.put(new Integer(CodePoint.RDBACCRM), "RDBACCRM");
         codePointToName.put(new Integer(CodePoint.RDBALWUPD), "RDBALWUPD");
         codePointToName.put(new Integer(CodePoint.RDBATHRM), "RDBATHRM");
         codePointToName.put(new Integer(CodePoint.RDBCMM), "RDBCMM");
         codePointToName.put(new Integer(CodePoint.RDBCMTOK), "RDBCMTOK");
         codePointToName.put(new Integer(CodePoint.RDBNACRM), "RDBNACRM");
         codePointToName.put(new Integer(CodePoint.RDBNAM), "RDBNAM");
         codePointToName.put(new Integer(CodePoint.RDBNFNRM), "RDBNFNRM");
         codePointToName.put(new Integer(CodePoint.RDBRLLBCK), "RDBRLLBCK");
         codePointToName.put(new Integer(CodePoint.RDBUPDRM), "RDBUPDRM");
         codePointToName.put(new Integer(CodePoint.REBIND), "REBIND");
         codePointToName.put(new Integer(CodePoint.RSCLMTRM), "RSCLMTRM");
         codePointToName.put(new Integer(CodePoint.RSLSETRM), "RSLSETRM");
         codePointToName.put(new Integer(CodePoint.RTNEXTDTA), "RTNEXTDTA");
         codePointToName.put(new Integer(CodePoint.RTNSQLDA), "RTNSQLDA");
         codePointToName.put(new Integer(CodePoint.SECCHK), "SECCHK");
         codePointToName.put(new Integer(CodePoint.SECCHKCD), "SECCHKCD");
         codePointToName.put(new Integer(CodePoint.SECCHKRM), "SECCHKRM");
         codePointToName.put(new Integer(CodePoint.SECMEC), "SECMEC");
         codePointToName.put(new Integer(CodePoint.SECMGRNM), "SECMGRNM");
         codePointToName.put(new Integer(CodePoint.SECTKN), "SECTKN");
         codePointToName.put(new Integer(CodePoint.SPVNAM), "SPVNAM");
         codePointToName.put(new Integer(CodePoint.SQLAM), "SQLAM");
         codePointToName.put(new Integer(CodePoint.SQLATTR), "SQLATTR");
         codePointToName.put(new Integer(CodePoint.SQLCARD), "SQLCARD");
         codePointToName.put(new Integer(CodePoint.SQLERRRM), "SQLERRRM");
         codePointToName.put(new Integer(CodePoint.SQLDARD), "SQLDARD");
         codePointToName.put(new Integer(CodePoint.SQLDTA), "SQLDTA");
         codePointToName.put(new Integer(CodePoint.SQLDTARD), "SQLDTARD");
         codePointToName.put(new Integer(CodePoint.SQLSTT), "SQLSTT");
         codePointToName.put(new Integer(CodePoint.SQLSTTVRB), "SQLSTTVRB");
         codePointToName.put(new Integer(CodePoint.SRVCLSNM), "SRVCLSNM");
         codePointToName.put(new Integer(CodePoint.SRVRLSLV), "SRVRLSLV");
         codePointToName.put(new Integer(CodePoint.SRVNAM), "SRVNAM");
         codePointToName.put(new Integer(CodePoint.SVRCOD), "SVRCOD");
         codePointToName.put(new Integer(CodePoint.SYNCCTL), "SYNCCTL");
         codePointToName.put(new Integer(CodePoint.SYNCLOG), "SYNCLOG");
         codePointToName.put(new Integer(CodePoint.SYNCRSY), "SYNCRSY");
         codePointToName.put(new Integer(CodePoint.SYNTAXRM), "SYNTAXRM");
         codePointToName.put(new Integer(CodePoint.TRGNSPRM), "TRGNSPRM");
         codePointToName.put(new Integer(CodePoint.TYPDEFNAM), "TYPDEFNAM");
         codePointToName.put(new Integer(CodePoint.TYPDEFOVR), "TYPDEFOVR");
         codePointToName.put(new Integer(CodePoint.TYPSQLDA), "TYPSQLDA");
         codePointToName.put(new Integer(CodePoint.UOWDSP), "UOWDSP");
         codePointToName.put(new Integer(CodePoint.USRID), "USRID");
         codePointToName.put(new Integer(CodePoint.VALNSPRM), "VALNSPRM");
         codePointToName.put(new Integer(CodePoint.PBSD), "PBSD");
         codePointToName.put(new Integer(CodePoint.PBSD_ISO), "PBSD_ISO");
         codePointToName.put(new Integer(CodePoint.PBSD_SCHEMA), "PBSD_SCHEMA");
         codePointToName.put(new Integer(CodePoint.UNICODEMGR), "UNICODEMGR");

         for (Integer key: codePointToName.keySet()) {
             nameToCodePoint.put(codePointToName.get(key), key);
         }

    }

      public static String lookup (int codePoint) {
        return codePointToName.get(new Integer (codePoint));
    }

    public static Integer lookup (String name) {
        return nameToCodePoint.get(name);
    }

}
