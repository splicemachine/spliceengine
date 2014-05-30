package com.splicemachine.derby.management;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;

/**
 * Created by jyuan on 5/13/14.
 */

public class XPlainTraceLegend {

    private Map<String, LegendEntry> legendMap;
    private static final int LENGTH = 30;
    public XPlainTraceLegend() {

        legendMap = new TreeMap<String, LegendEntry>();
        populateLegendMap();
    }


    private void populateLegendMap() {
        legendMap.put("HOST", new LegendEntry("HOST", "H", false));
        legendMap.put("REGION", new LegendEntry("REGION", "R", false));
        legendMap.put("TOTALWALLTIME", new LegendEntry("TOTALWALLTIME", "TWT", false));
        legendMap.put("TOTALUSERTIME", new LegendEntry("TOTALUSERTIME", "TUT", false));
        legendMap.put("TOTALCPUTIME",  new LegendEntry("TOTALCPUTIME", "TCT", false));

        legendMap.put("LOCALSCANROWS",  new LegendEntry("LOCALSCANROWS", "LSR", false));
        legendMap.put("LOCALSCANBYTES",  new LegendEntry("LOCALSCANBYTES", "LSB", false));

        legendMap.put("LOCALSCANWALLTIME",  new LegendEntry("LOCALSCANWALLTIME", "LSWT", false));
        legendMap.put("LOCALSCANCPUTIME",  new LegendEntry("LOCALSCANCPUTIME", "LSCT", false));
        legendMap.put("LOCALSCANUSERTIME",  new LegendEntry("LOCALSCANUSERTIME", "LSUT", false));

        legendMap.put("REMOTESCANROWS",  new LegendEntry("REMOTESCANROWS", "RSR", false));
        legendMap.put("REMOTESCANBYTES",  new LegendEntry("REMOTESCANBYTES", "RSB", false));

        legendMap.put("REMOTESCANWALLTIME",  new LegendEntry("REMOTESCANWALLTIME", "RSWT", false));
        legendMap.put("REMOTESCANCPUTIME",  new LegendEntry("REMOTESCANCPUTIME", "RSCP", false));
        legendMap.put("REMOTESCANUSERTIME",  new LegendEntry("REMOTESCANUSERTIME", "RSUT", false));

        legendMap.put("REMOTEGETROWS",  new LegendEntry("REMOTEGETROWS", "RGR", false));
        legendMap.put("REMOTEGETBYTES",  new LegendEntry("REMOTEGETBYTES", "RGB", false));

        legendMap.put("REMOTEGETWALLTIME",  new LegendEntry("REMOTEGETWALLTIME", "RGWT", false));
        legendMap.put("REMOTEGETCPUTIME",  new LegendEntry("REMOTEGETCPUTIME", "RGCT", false));
        legendMap.put("REMOTEGETUSERTIME",  new LegendEntry("REMOTEGETUSERTIME", "RGUT", false));

        legendMap.put("WRITEROWS",  new LegendEntry("WRITEROWS", "WR", false));
        legendMap.put("WRITEBYTES",  new LegendEntry("WRITEBYTES", "WB", false));

        legendMap.put("PROCESSINGWALLTIME",  new LegendEntry("PROCESSINGWALLTIME", "PWT", false));
        legendMap.put("PROCESSINGCPUTIME",  new LegendEntry("PROCESSINGCPUTIME", "PCT", false));
        legendMap.put("PROCESSINGUSERTIME",  new LegendEntry("PROCESSINGUSERTIME", "PUT", false));

        legendMap.put("FILTEREDROWS",  new LegendEntry("FILTEREDROWS", "FR", false));
        legendMap.put("TASKQUEUEWAITWALLTIME",  new LegendEntry("TASKQUEUEWAITWALLTIME", "TQWWT", false));
        legendMap.put("STARTTIMESTAMP",  new LegendEntry("STARTTIMESTAMP", "ST", false));
        legendMap.put("STOPTIMESTAMP",  new LegendEntry("STOPTIMESTAMP", "PT", false));

        legendMap.put("INPUTROWS",  new LegendEntry("INPUTROWS", "IR", false));
        legendMap.put("OUTPUTROWS",  new LegendEntry("OUTPUTROWS", "OR", false));

        legendMap.put("WRITESLEEPWALLTIME",  new LegendEntry("WRITESLEEPWALLTIME", "WSWT", false));
        legendMap.put("WRITESLEEPCPUTIME",  new LegendEntry("WRITESLEEPCPUTIME", "WSCT", false));
        legendMap.put("WRITESLEEPUSERTIME",  new LegendEntry("WRITESLEEPUSERTIME", "WSUT", false));

        legendMap.put("REJECTEDWRITEATTEMPTS",  new LegendEntry("REJECTEDWRITEATTEMPTS", "RJWA", false));
        legendMap.put("RETRIEDWRITEATTEMPTS",  new LegendEntry("RETRIEDWRITEATTEMPTS", "RTWA", false));
        legendMap.put("FAILEDWRITEATTEMPTS",  new LegendEntry("FAILEDWRITEATTEMPTS", "FWA", false));
        legendMap.put("PARTIALWRITEFAILURES",  new LegendEntry("PARTIALWRITEFAILURESS", "PWF", false));

        legendMap.put("WRITENETWORKWALLTIME",  new LegendEntry("WRITENETWORKWALLTIME", "WNWT", false));
        legendMap.put("WRITENETWORKCPUTIME",  new LegendEntry("WRITENETWORKCPUTIMEE", "WNCT", false));
        legendMap.put("WRITENETWORKUSERTIME",  new LegendEntry("WRITENETWORKUSERTIME", "WNUT", false));

        legendMap.put("WRITETHREADEDWALLTIME",  new LegendEntry("WRITETHREADEDWALLTIME", "WTWT", false));
        legendMap.put("WRITETHREADEDUSERTIME",  new LegendEntry("WRITETHREADEDUSERTIME", "WTUT", false));
        legendMap.put("WRITETHREADEDCPUTIME",  new LegendEntry("WRITETHREADEDCPUTIME", "WTCT", false));
        legendMap.put("ITERATIONS",  new LegendEntry("ITERATIONS", "ITR", false));
        legendMap.put("INFO",  new LegendEntry("INFO", "IF", false));
    }

    public String getShortName(String fullName) {
        LegendEntry entry = legendMap.get(fullName);
        return entry.getShortName();
    }

    public void use(String fullName) {
        LegendEntry entry = legendMap.get(fullName);
        entry.setUsed(true);
    }

    public void print(ArrayList<ExecRow> rows, ExecRow dataTemplate) throws StandardException{

        StringBuilder sb = new StringBuilder();
        Set<String> keys = legendMap.keySet();
        int i = 0;
        for (String key:keys) {
            LegendEntry entry = legendMap.get(key);
            if (entry.isUsed()) {
                sb.append(entry.getShortName()).append(": ").append(entry.getFullName());
                i++;
                if (i < 3) {
                    int len = sb.length();
                    int k = i*LENGTH - len;
                    for (int j = 0; j < k; ++j) {
                        sb.append(" ");
                    }
                }
                else {
                    i = 0;
                    dataTemplate.resetRowArray();
                    DataValueDescriptor[] dvds = dataTemplate.getRowArray();
                    dvds[0].setValue(sb.toString());
                    rows.add(dataTemplate.getClone());
                    sb = new StringBuilder();
                }
            }
        }
        if (sb.length() > 0) {
            dataTemplate.resetRowArray();
            DataValueDescriptor[] dvds = dataTemplate.getRowArray();
            dvds[0].setValue(sb.toString());
            rows.add(dataTemplate.getClone());
        }
    }

    private class LegendEntry {
        String fullName;
        String shortName;
        boolean used;

        public LegendEntry (String fullName, String shortName, boolean used) {
            this.fullName = fullName;
            this.shortName = shortName;
            this.used = used;
        }

        public void setFullName (String fullName) {this.fullName = fullName;}

        public String getFullName () {return fullName;}

        public void setShortName(String shortName) {this.shortName = shortName;}

        public String getShortName() {return shortName;}

        public void setUsed(boolean used) {this.used = used;}

        public boolean isUsed() {return used;}
    }
}
