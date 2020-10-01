package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.impl.sql.compile.ExplainNode;
import com.splicemachine.utils.IndentedString;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.*;

class SparkExplain implements Serializable {
    private static final long serialVersionUID = 1L;
    public ExplainNode.SparkExplainKind sparkExplainKind = ExplainNode.SparkExplainKind.NONE;
    public LinkedList<IndentedString> explainStrings = new LinkedList<>();
    public LinkedList<IndentedString> tempOperationStrings = new LinkedList<>();
    public LinkedList<List<IndentedString>> stashedSpliceOperationStrings = new LinkedList<>();
    public LinkedList<List<IndentedString>> spliceOperationStrings = new LinkedList<>();

    public TreeMap<Integer, Integer> numLeadingSpaces = new TreeMap<>();
    public TreeMap<Integer, String> spacesMap = new TreeMap<>();

    // The depth of the current operation being processed via getDataSet
    // in the operation tree.
    public int opDepth = 0;
    public boolean isSparkExplain() { return sparkExplainKind != ExplainNode.SparkExplainKind.NONE; }
    public ExplainNode.SparkExplainKind getSparkExplainKind() { return sparkExplainKind; }
    public void setSparkExplain(ExplainNode.SparkExplainKind newValue) { sparkExplainKind = newValue; }
    private void prependIndentedStrings(List<IndentedString> indentedStrings) {
        tempOperationStrings.addAll(0, indentedStrings);
    }
    public void prependSpliceExplainString(String explainString) {
        StringBuilder sb = new StringBuilder();
        if (!explainString.isEmpty())
            sb.append("-> ");
        // Strip out newlines and trailing spaces.
        sb.append(explainString.replace("\n","").replaceFirst("\\s++$", ""));
        tempOperationStrings.addFirst(new IndentedString(getOpDepth(), sb.toString()));
    }
    public void appendSpliceExplainString(String explainString) {
        StringBuilder sb = new StringBuilder();
        if (!explainString.isEmpty())
            sb.append("-> ");
        // Strip out newlines and trailing spaces.
        sb.append(explainString.replace("\n","").replaceFirst("\\s++$", ""));
        tempOperationStrings.addLast(new IndentedString(getOpDepth(), sb.toString()));
    }
    public void finalizeTempOperationStrings() {
        if (!tempOperationStrings.isEmpty()) {
            spliceOperationStrings.addFirst(tempOperationStrings);
            tempOperationStrings = new LinkedList<>();
        }
    }
    private void stashTempOperationStrings() {
        if (!tempOperationStrings.isEmpty()) {
            stashedSpliceOperationStrings.addFirst(tempOperationStrings);
            tempOperationStrings = new LinkedList<>();
        }
    }

    private void popStashedOperationStrings() {
        if (!stashedSpliceOperationStrings.isEmpty()) {
            spliceOperationStrings.addAll(0, stashedSpliceOperationStrings);
            stashedSpliceOperationStrings = new LinkedList<>();
        }
    }

    private boolean topOfSavedOperationsIsSibling(IndentedString newSparkExplain) {
        if (!spliceOperationStrings.isEmpty() &&
                spliceOperationStrings.get(0).get(0).getIndentationLevel() == newSparkExplain.getIndentationLevel())
            return true;
        return false;
    }

    public void popSpliceOperation() {
        if (!spliceOperationStrings.isEmpty() &&
                spliceOperationStrings.get(0).get(0).getIndentationLevel() == opDepth+1)
            tempOperationStrings.addAll(0, spliceOperationStrings.remove(0));
    }
    public void prependSparkExplainStrings(List<String> stringsToAdd, boolean firstOperationSource, boolean lastOperationSource) {
        if (firstOperationSource)
            finalizeTempOperationStrings();
        IndentedString newSparkExplain =
                new IndentedString(getOpDepth()+1, stringsToAdd);
        newSparkExplain = fixupSparkExplain(newSparkExplain);
        boolean isSecondSourceOfJoin = !firstOperationSource && lastOperationSource;
        boolean siblingPresent = isSecondSourceOfJoin &&
                tempOperationStrings.size() >= 1 &&
                tempOperationStrings.get(0).getIndentationLevel() ==
                        newSparkExplain.getIndentationLevel();

        // Handle join cases where there is still one element left in
        // tempOperationStrings at the same of higher level as the spark operation,
        // which needs to get tied together via a join.
        if (tempOperationStrings.isEmpty() ||
                tempOperationStrings.size() >= 1 &&
                        tempOperationStrings.get(0).getIndentationLevel() <=
                                newSparkExplain.getIndentationLevel()) {
            if (!siblingPresent) {
                finalizeTempOperationStrings();
                if (isSecondSourceOfJoin &&
                        topOfSavedOperationsIsSibling(newSparkExplain))
                    popSpliceOperation();
            }
            tempOperationStrings.add(new IndentedString(getOpDepth() + 1, "-> NativeSparkDataSet"));
            newSparkExplain.setIndentationLevel(getOpDepth() + 2);
            tempOperationStrings.add(newSparkExplain);
        }
        else
            explainStrings.addFirst(newSparkExplain);
    }

    private IndentedString
    fixupSparkExplain(IndentedString sparkExplain) {
        finalizeTempOperationStrings();
        if (spliceOperationStrings.isEmpty())
            return sparkExplain;

        IndentedString newSparkExplain = null;
        LinkedList<String> newTextLines = new LinkedList<>();
        for (String string:sparkExplain.getTextLines()) {
            newTextLines.add(string);
            int matchIndex =
                    StringUtils.indexOfAny(string, new String[]{"Scan ExistingRDD", "FileScan", "LogicalRDD", "Relation[", "ReusedExchange"});

            int matchReusedExchange =
                    StringUtils.indexOfAny(string, new String[]{"ReusedExchange"});

            if (!spliceOperationStrings.isEmpty() && matchIndex != -1) {
                List<IndentedString> list = spliceOperationStrings.removeLast();
                int baseIndentationLevel = list.get(0).getIndentationLevel();
                int leadingSpaces = matchIndex;
                {
                    // Native spark explain is indented 3 spaces in getNativeSparkExplain().
                    int numSpaces = leadingSpaces + 3;

                    char[] charArray = new char[numSpaces];
                    Arrays.fill(charArray, ' ');
                    String prependString = new String(charArray);
                    // Adjust the global indentation setting in cases we are dealing
                    // with one source of a join, and the other source will
                    // be processed later.
                    numLeadingSpaces.put(baseIndentationLevel, 1);
                    numLeadingSpaces.put(baseIndentationLevel+1, prependString.length() + sparkExplain.getIndentationLevel()*2);
                    spacesMap.put(baseIndentationLevel, prependString);
                    for (Integer i = numLeadingSpaces.higherKey(baseIndentationLevel); i != null;
                         i = numLeadingSpaces.higherKey(i))
                        numLeadingSpaces.remove(i);
                    for (Integer i = spacesMap.higherKey(baseIndentationLevel); i != null;
                         i = spacesMap.higherKey(i))
                        spacesMap.remove(i);

                }
                boolean notMatched;
                do {
                    ListIterator<IndentedString> iter = list.listIterator();
                    notMatched = true;

                    IndentedString firstStringInOperationSet = list.get(0);

                    if (firstStringInOperationSet != null &&
                            firstStringInOperationSet.getIndentationLevel() <= getOpDepth() + 1) {

                        stashTempOperationStrings();
                        prependIndentedStrings(list);
                    }
                    else
                        while (iter.hasNext()) {
                            IndentedString istr = iter.next();
                            notMatched = false;
                            if (matchReusedExchange == -1)
                                for (String str : istr.getTextLines()) {
                                    int indentationLevel = istr.getIndentationLevel() - baseIndentationLevel;
                                    int numExtraSpaces = indentationLevel * 2;
                                    int numSpaces = leadingSpaces + numExtraSpaces;
                                    char[] charArray = new char[numSpaces];
                                    Arrays.fill(charArray, ' ');
                                    String prependString = new String(charArray);
                                    newTextLines.add(prependString + str);
                                }
                        }
                    if (notMatched && !spliceOperationStrings.isEmpty()) {
                        list = spliceOperationStrings.removeLast();
                        baseIndentationLevel = list.get(0).getIndentationLevel();
                    }
                    else
                        notMatched = false;
                } while (notMatched);
            }
        }
        spacesMap.clear();
        stashTempOperationStrings();
        popStashedOperationStrings();
        newSparkExplain = new IndentedString(sparkExplain.getIndentationLevel(), newTextLines);
        return newSparkExplain;
    }

    private int findIndentation(Map<Integer, Integer>  numLeadingSpaces,
                                int indentationLevel,
                                boolean doLookup) {
        Integer numSpaces = null;
        if (doLookup)
            numSpaces = numLeadingSpaces.get(indentationLevel);

        if (numSpaces == null && doLookup) {
            for (int i = indentationLevel; i >= 0; i--) {
                Integer foundSpaces = numLeadingSpaces.get(i);
                if (foundSpaces == null)
                    continue;
                numSpaces = foundSpaces + (indentationLevel - i) * 2;
                numLeadingSpaces.put(indentationLevel, numSpaces);
                break;
            }
        }
        if (numSpaces == null)
            numSpaces = indentationLevel * 2;

        return numSpaces;
    }

    public List<String> getNativeSparkExplain() {
        int indentationLevel = 0;

        finalizeTempOperationStrings();
        for (List<IndentedString> indentedStrings:spliceOperationStrings)
            explainStrings.addAll(0, indentedStrings);

        if (!explainStrings.isEmpty())
            indentationLevel = explainStrings.getFirst().getIndentationLevel();

        numLeadingSpaces.put(indentationLevel, 0);
        spacesMap.put(indentationLevel, "");

        int previousIndentationLevel = -1;
        int maxIndentationLevel = -1;
        List<String> sparkExplain = new LinkedList<>();
        boolean firstLine = true;
        for (IndentedString strings:explainStrings) {
            if (strings.getIndentationLevel() <= previousIndentationLevel) {
                for (int i = maxIndentationLevel; i >= strings.getIndentationLevel(); i--) {
                    spacesMap.remove(i);
                    numLeadingSpaces.remove(i);
                }
            }
            String prependString = spacesMap.get(strings.getIndentationLevel());
            boolean nativeSpark = strings.getTextLines().size() > 1;
            if (prependString == null)
            {
                int indentation = findIndentation(numLeadingSpaces,
                        strings.getIndentationLevel(),
                        nativeSpark);
                char[] charArray = new char[indentation];
                Arrays.fill(charArray, ' ');
                prependString = new String(charArray);
                spacesMap.put(strings.getIndentationLevel(), prependString);
            }
            if (nativeSpark) {
                if (firstLine)
                    sparkExplain.add(prependString + "NativeSparkDataSet");
                else if (!sparkExplain.get(sparkExplain.size()-1).contains("NativeSparkDataSet"))
                    sparkExplain.add(prependString + "-> NativeSparkDataSet");
                prependString = prependString + "   ";
            }
            int newIndentPos = prependString.length();
            for (String s:strings.getTextLines()) {
                String newString = prependString + s;
                sparkExplain.add(newString);
                int tempIndentPos = newString.indexOf("+-");
                if (tempIndentPos > newIndentPos) {
                    newIndentPos = tempIndentPos;
                    numLeadingSpaces.put(strings.getIndentationLevel(), newIndentPos);
                }
            }
            previousIndentationLevel = strings.getIndentationLevel();
            if (previousIndentationLevel > maxIndentationLevel)
                maxIndentationLevel = previousIndentationLevel;
            firstLine = false;
        }
        return sparkExplain;
    }

    public int getOpDepth() { return opDepth; }

    public void incrementOpDepth() {
        if (isSparkExplain())
            opDepth++;
    }

    public void decrementOpDepth() {
        if (isSparkExplain())
            opDepth--;
    }

    public void resetOpDepth() { opDepth = 0; }

}
