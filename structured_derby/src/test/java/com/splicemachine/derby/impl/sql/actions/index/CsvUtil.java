package com.splicemachine.derby.impl.sql.actions.index;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * CSV test file manipulation
 */
public class CsvUtil {

    public static final String CSV_FILE_LOC = "/index/";

    @Test
    public void makeCustomerUnique() throws Exception{
        String dirName = getResourceDirectory() + CSV_FILE_LOC;
        String sourceFile = "customer.csv";
        String targetFile = "customer-unique.csv";
        int[] pk = new int[] {0,1,2};
        writeLines(dirName, targetFile, makeUnique(dirName,sourceFile,pk));
    }

    @Test
    public void makeOrderUnique() throws Exception{
        String dirName = getResourceDirectory() + CSV_FILE_LOC;
        String sourceFile = "order.csv";
        String targetFile = "order-unique.csv";
        int[] pk = new int[] {0,1,2};
        writeLines(dirName, targetFile, makeUnique(dirName,sourceFile,pk));
    }

    @Test
    public void giveOrderSomeNulls() throws Exception {
        String dirName = getResourceDirectory() + CSV_FILE_LOC;
        String sourceFile = "order.csv";
        String targetFile = "order-with-nulls.csv";
        writeLines(dirName, targetFile, insertString(dirName, sourceFile, 5, ""));
    }

    @Test
    public void changeOrderLineDecimalFormat() throws Exception {
        String dirName = getResourceDirectory() + CSV_FILE_LOC;
        String sourceFile = "order-line.csv";
        String targetFile = "order-line-decimal.csv";
        writeLines(dirName, targetFile, insertString(dirName, sourceFile, 9, "5.0"));
    }

    private Collection<String> insertString(String dirName, String fileName, int colNum, String chars) {
        List<String> lines = fileToLines(dirName + fileName, "--");
        List<String> outLines = new ArrayList<String>(lines.size());

        StringBuilder aLine = new StringBuilder();
        for (String line : lines) {
            String[] cols = line.split(",");
            if (cols.length <= colNum-1) {
                throw new RuntimeException("Index "+(colNum-1)+" is greater than size - "+cols.length);
            }
            aLine.setLength(0);
            for (int i=0; i<cols.length; i++) {
                if (i == colNum-1) {
                    aLine.append(chars).append(",");
                } else {
                    aLine.append(cols[i]).append(",");
                }
            }
            outLines.add(aLine.toString());
        }

        return outLines;
    }

    public static Collection<String> makeUnique(String dirName, String fileName, int[] colNums) {
        List<String> lines = fileToLines(dirName + fileName, "--");
        Map<String, String> unique = new HashMap<String, String>(lines.size());

        StringBuilder key = new StringBuilder();
        for (String line : lines) {
            String[] cols = line.split(",");
            if (cols.length <= max(colNums)) {
                throw new RuntimeException("Index "+max(colNums)+" is greater than size - "+cols.length);
            }
            key.setLength(0);
            for (int i=0; i<colNums.length; i++) {
                key.append(cols[colNums[i]]).append("|");
            }
            if (unique.containsKey(key.toString())) {
                System.out.println("Collision on key: "+key.toString());
            }
            unique.put(key.toString(), line);
        }
        return unique.values();
    }

    public static void writeLines(String dirName, String fileName, Collection<String> content) throws Exception {
        File targetFile = new File(dirName,fileName);
        Files.createParentDirs(targetFile);
        if (targetFile.exists())
            targetFile.delete();
        targetFile.createNewFile();
        FileUtils.writeLines(targetFile, content);
    }

    private static int max(int[] nums) {
        int max = 0;
        for (int i=0; i<nums.length; i++) {
            if (nums[i] >= max) {
                max = nums[i];
            }
        }
        return max;
    }
    private static List<String> fileToLines(String filePath, String commentPattern) {
        List<String> lines = new LinkedList<String>();
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(filePath));

            String line = in.readLine();
            while(line != null) {
                if (commentPattern != null) {
                    if (! lineIsComment(line, commentPattern)) {
                        lines.add(line);
                    }
                } else {
                    lines.add(line);
                }
                line = in.readLine();
            }
        } catch (IOException e) {
            Assert.fail("Unable to read: " + filePath + ": " + e.getLocalizedMessage());
        } finally {
        	if (in != null) {
        		try {
					in.close();
				} catch (IOException e) {
					// ignore
				}
        	}
        }
        return lines;
    }

    private static boolean lineIsComment(String line, String commentPattern) {
        if (commentPattern == null || commentPattern.isEmpty()) {
            return false;
        }
        return line.trim().startsWith(commentPattern);
    }

    private static String getResourceDirectory() {
        String userDir = System.getProperty("user.dir");
        if(!userDir.endsWith("structured_derby"))
            userDir = userDir+"/structured_derby";
        return userDir+"/src/test/resources";
    }

}
