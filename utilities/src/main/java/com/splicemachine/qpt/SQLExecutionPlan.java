package com.splicemachine.qpt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLExecutionPlan {

    private String planID;      // plan signature
    private String shapeID;     // shape signature
    private String scaleID;     // scale signature

    static class ParseException extends RuntimeException {
        ParseException(String msg) {
            super(msg);
        }
    }

    private SQLExecutionPlan(TreeNode root) {
        String prefix = (root.hasJoin() ? "J" : "E") + (root.isOLTP() ? "T" : "A");
        planID = Encoding.makeId(prefix, root.computePlanHash());
        shapeID = Encoding.makeId("X", root.computeShapeHash());
        scaleID = Encoding.makeId("Y", root.computeScaleHash());
    }

    public String getPlanID() {
        return planID;
    }

    public String getShapeID() {
        return shapeID;
    }

    public String getScaleID() {
        return scaleID;
    }

    private static long updateHash(long hash, long value) {
        return 31 * hash + value;
    }

    private static long updateHash(long hash, String value) {
        return value != null ? updateHash(hash, value.hashCode()) : hash;
    }

    private static final String ENGINE_ATTR = "engine";
    private static final String ENGINE_ATTRX = "engineX";
    private static final String TABLE_ATTR = "table";
    private static final String PREDS_ATTR = "preds";

    static class TreeNode {
        String operation;
        HashMap<String, String> attributes;
        ArrayList<TreeNode> children;

        void toStringBuilder(StringBuilder sb, int depth) {
            for(int i=0; i<depth; i++) sb.append(' ');
            sb.append(operation);
            sb.append(" " + attributes.toString());
            sb.append("\n");
            for(TreeNode child : children) {
                child.toStringBuilder(sb, depth+1);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            toStringBuilder(sb, 0);
            return sb.toString();
        }

        TreeNode(String op) {
            this.operation = op;
            this.attributes = new HashMap<>();
            this.children = new ArrayList<>();
        }

        void addAttribute(String name, String value) {
            attributes.put(name, value);
        }

        long computeShapeHash() {
            long hashCode = operation.hashCode();

            for (TreeNode child : children) {
                hashCode = 31 * hashCode + child.computeShapeHash();
            }
            return hashCode;
        }

        long computePlanHash() {
            long hashCode = operation.hashCode();

            // Include table/index name if any
            hashCode = updateHash(hashCode, attributes.get(TABLE_ATTR));

            // Include all column names from the predicates
            String preds = attributes.get(PREDS_ATTR);
            if (preds != null) {
                Pattern pattern = Pattern.compile("([a-zA-Z_#0-9]+)\\[\\d+:\\d+\\]");
                Matcher matcher = pattern.matcher(preds);
                while (matcher.find()) {
                    hashCode = updateHash(hashCode, matcher.group(1));
                }
            }

            // Include children
            for (TreeNode child : children) {
                hashCode = 31 * hashCode + child.computePlanHash();
            }
            return hashCode;
        }

        long computeScaleHash() {
            // Not implemented
            return -1;
        }

        boolean hasJoin() {
            if (operation.endsWith("Join")) {
                return true;
            }
            for (TreeNode child : children) {
                if (child.hasJoin()) {
                    return true;
                }
            }
            return false;
        }

        boolean isOLTP() {
            String engine = attributes.get(ENGINE_ATTR);
            return (engine != null && engine.equals("OLTP"));
        }
    }

    static class PlanParser {
        String input;
        int idx;

        PlanParser(String line) {
            this.input = line;
            idx = 0;
        }

        int parseDepth() {
            if (idx != 0) throw new ParseException("ERROR: PlanParser.parseDepth() called out-of-order");
            skipWhitespaces();
            int depth = idx / 2;
            if (idx > 0) {
                if (input.charAt(idx++) != '-' || input.charAt(idx++) != '>') {
                    throw new ParseException("ERROR: PlanParser.parseDepth() malformed line:\n" + input);
                }
            }
            return depth;
        }

        void skipWhitespaces() {
            while (idx < input.length() && Character.isWhitespace(input.charAt(idx))) ++idx;
        }

        void skipTo(String token) {
            int nidx = input.indexOf(token, idx);
            if (nidx < 0) idx = input.length();
            idx = nidx + token.length();
        }

        String parseTo(String... tokens) {
            skipWhitespaces();
            for (String token : tokens) {
                int eidx = input.indexOf(token, idx);
                if (eidx >= 0) {
                    String result = input.substring(idx, eidx);
                    idx = eidx + token.length();
                    return result;
                }
            }
            String result = input.substring(idx);
            idx = input.length();
            return result;
        }

        String parseName() {
            skipWhitespaces();
            StringBuilder sb = new StringBuilder();
            for (int idx0 = idx; idx < input.length(); ++idx) {
                char ch = input.charAt(idx);
                if (idx == idx0) {
                    if (!Character.isAlphabetic(ch)) break;
                }
                else if (!Character.isLetterOrDigit(ch) && ch != '_') break;
                sb.append(ch);
            }
            return sb.toString();
        }

        boolean parseToken(String token, boolean allowSpaces) {
            if (allowSpaces) skipWhitespaces();
            if (input.startsWith(token, idx)) {
                idx += token.length();
                return true;
            }
            return false;
        }

        String parseBracket(char end) {
            char bgn;
            switch (end) {
                case ')':   bgn = '('; break;
                case ']':   bgn = '['; break;
                case '}':   bgn = '{'; break;
                default:
                    throw new ParseException("ERROR: parseBracket: wrong parameter: " + end);
            }
            int level = 1;
            int idx0 = idx;
            for (; idx < input.length(); ++idx) {
                char ch = input.charAt(idx);
                if (ch == bgn) {
                    ++level;
                }
                else if (ch == end) {
                    if (--level == 0) {
                        return input.substring(idx0, idx++);
                    }
                }
            }
            return input.substring(idx0);
        }

        boolean parseToken(String token) {
            return parseToken(token, true);
        }

        boolean eof() {
            return idx >= input.length();
        }
    }

    private static void parseLine(List<TreeNode> stack, String line) throws ParseException {
        PlanParser parser = new PlanParser(line);
        int depth = parser.parseDepth();
        String op = parser.parseName();

        // make sure stack is big enough
        for(int i=stack.size(); i<depth+1; i++)
            stack.add(null);

        TreeNode node = new TreeNode(op);
        if (depth > 0) {
            TreeNode parent = stack.get(depth - 1);
            if(parent == null)
                throw new ParseException("no parent");
            parent.children.add(node);
        }
        stack.set(depth, node);

        if (parser.parseToken("[")) {
            String tableName = parser.parseName();
            node.addAttribute(TABLE_ATTR, tableName);
            parser.skipTo("]");
        }
        if (parser.parseToken("(")) {
            while (!parser.eof()) {
                String name = parser.parseName();
                if (name.isEmpty()) break;
                if (!parser.parseToken("=")) {
                    node.addAttribute(name, parser.parseTo(",", ")"));
                    continue;
                }
                if (ENGINE_ATTR.equals(name)) {
                    String value = parser.parseName();
                    node.addAttribute(ENGINE_ATTR, value);
                    value = parser.parseTo(",", ")");
                    node.addAttribute(ENGINE_ATTRX, value);
                }
                else {
                    if (parser.parseToken("[")) {
                        node.addAttribute(name, parser.parseBracket(']'));
                    } else {
                        node.addAttribute(name, parser.parseTo(",", ")"));
                    }
                }
            }
            parser.skipTo(")");
        }
    }

    static TreeNode parseTreeNodes( List<String> plan) throws ParseException {
        List<TreeNode> stack = new ArrayList<TreeNode>(255);
        for (int i = 0; i < plan.size(); ++i) {
            String line = plan.get(i);
            if (i > 0 && !line.matches("^ +-> .*")) {
                break;
            }
            parseLine(stack, line);
        }
        return stack.get(0);
    }

    public static SQLExecutionPlan parse(List<String> plan) throws ParseException {
        return new SQLExecutionPlan(parseTreeNodes(plan) );
    }

    @Override
    public String toString() {
        return "planID=" + planID + ", shapeID=" + shapeID + ", scaleID=" + scaleID;
    }
}
