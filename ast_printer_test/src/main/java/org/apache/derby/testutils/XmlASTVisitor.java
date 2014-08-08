package org.apache.derby.testutils;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ContextId;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.compile.ASTVisitor;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.jdbc.EmbedConnection;

/**
 * A little visitor to run the Derby parser, bind, optimizer on a text string and to print
 * the resulting abstract syntax tree.
 */
public class XmlASTVisitor implements ASTVisitor, ASTVisitorConfig {
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Continue through plan phase after parsing. <i>Warning</i>: setting
     * this property will cause table/column references will be bound so
     * you must have schema, table, etc, populated.
     */
    public static final String CONTINUE_AFTER_PARSE = "ContinueAfterParse";

    private static final String DERBY_DEBUG_SETTING = "derby.debug.true";
    private static final String STOP_AFTER_PARSING = "StopAfterParsing";
    private static final String STOPPED_AFTER_PARSING = "42Z55";
    private static final String INDENTATION = "    ";

    private static final String XML_BOILERPLATE = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

    // elements and attributes

    public static final String QUERY_TREE = "queryTree";
    public static final String QUERY_TEXT = "queryText";
    public static final String NODE = "node";
    public static final String NODE_TYPE_ATTR = "type";
    public static final String CONTENTS = "contents";
    public static final String MEMBER = "member";
    public static final String NAME_ATTR = "name";
    public static final String VALUE_ATTR = "value";


    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private Connection _connection;
    private PrintStream _printStream = System.out;
    private HashSet<Visitable> _visitedNodes;

    private ArrayList<String> _tagStack;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public XmlASTVisitor() {}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ENTRY POINT
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Entry point. Takes one argument: the string to be parsed.
     *
     * @param query the query string
     * @throws Exception any problem
     */
    public ResultSet execute(String query) throws Exception {

        ContextManager contextManager = ((EmbedConnection) _connection).getContextManager();
        LanguageConnectionContext lcc = (LanguageConnectionContext) contextManager.getContext(ContextId.LANG_CONNECTION);
        lcc.setASTVisitor(this);

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = _connection.prepareStatement(query);
            // TODO: exec query and return RS - getting NPE; snowflake null
//            rs = ps.executeQuery();
        } catch (SQLException se) {
            String sqlState = se.getSQLState();

            if (!STOPPED_AFTER_PARSING.equals(sqlState)) {
                throw se;
            }
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
        return rs;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Visitor BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    public Visitable visit(Visitable node) throws StandardException {
        if (!_visitedNodes.contains(node)) {
            _visitedNodes.add(node);

            Attr nodeType = new Attr(NODE_TYPE_ATTR, node.getClass().getName());

            beginTag(NODE, new Attr[]{nodeType});
            {
                beginTag(CONTENTS, null);
                {
                    printMembers(node);
                }
                endTag();   // CONTENTS

                node.accept(this);
            }
            endTag();   // NODE
        }

        return node;
    }

    public boolean stopTraversal() {
        return false;
    }

    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }


    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private void println(String text) {
        _printStream.println(text);
    }

    private void print(String text) {
        _printStream.print(text);
    }

    private void printThrowable(Throwable t) {
        t.printStackTrace(_printStream);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MEMBER MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * The javadoc for QueryTreeNode.toString() advises the programmer to format
     * the output as one member field per line. We take advantage of that
     * convention here and treat each line as a separate member field.
     * </p>
     */
    private void printMembers(Visitable node) {
        String raw = node.toString();
        StringReader sr = new StringReader(raw);
        LineNumberReader lnr = new LineNumberReader(sr);

        try {
            while (true) {
                String nextLine = lnr.readLine();
                if (nextLine == null) {
                    break;
                }

                printMember(nextLine);
            }

            lnr.close();
            sr.close();
        } catch (IOException ioe) {
            printThrowable(ioe);
        }
    }

    /**
     * <p>
     * Take a line of text of the form "name:value" and print it as a member
     * element with name and value attributes. If there is no ':' to separate the name from the value,
     * then the whole line is treated as a value.
     * </p>
     */
    private void printMember(String line) throws IOException {
        int splitIdx = line.indexOf(':');
        int endIdx = line.length();
        String value;
        Attr nameAttr = null;
        Attr valueAttr;

        if (splitIdx > 0) {
            String name = line.substring(0, splitIdx).trim();

            if (name.length() > 0) {
                nameAttr = new Attr(NAME_ATTR, name);
            }
        } else {
            splitIdx = -1;
        }

        splitIdx++;

        if (splitIdx >= endIdx) {
            value = "";
        } else {
            value = line.substring(splitIdx, endIdx).trim();
        }

        valueAttr = new Attr(VALUE_ATTR, value);

        Attr[] attributes;
        if (nameAttr == null) {
            attributes = new Attr[]{valueAttr};
        } else {
            attributes = new Attr[]{nameAttr, valueAttr};
        }

        writeTextElement(MEMBER, attributes, null);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // XML MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Indent and write an opening tag.
     * </p>
     */
    public void beginTag(String tag, Attr[] attributes) {
        indent();
        print("<");
        print(tag);
        printAttributes(attributes);
        println(">");

        _tagStack.add(tag);
    }

    /**
     * <p>
     * Print an array of attributes.
     * </p>
     */
    public void printAttributes(Attr[] attributes) {
        if (attributes != null) {

            for (Attr attr : attributes) {
                print(" ");
                print(attr.name);
                print("=");
                print(doubleQuote(attr.value));
            }
        }
    }

    /**
     * <p>
     * Wrap double quotes around a string.
     * </p>
     */
    public String doubleQuote(String raw) {
        return '"' + raw + '"';
    }

    /**
     * <p>
     * Indent and write a closing tag.
     * </p>
     */
    public void endTag() {
        String tag = _tagStack.remove(_tagStack.size() - 1);

        indent();

        println("</" + tag + ">");
    }

    /**
     * <p>
     * Indent and write a whole element
     * </p>
     */
    public void writeTextElement(String tag, Attr[] attributes, String text) {
        indent();
        print("<");
        print(tag);
        printAttributes(attributes);

        if (text != null) {
            print(">");
            print(text);
            println("</" + tag + ">");
        } else {
            println("/>");
        }
    }


    /**
     * <p>
     * Indent based on the depth of our tag nesting level.
     * </p>
     */
    public void indent() {
        for (String ignored : _tagStack) {
            print(INDENTATION);
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ASTVisitor Interface
    //
    ///////////////////////////////////////////////////////////////////////////////////

    @Override
    public void initializeVisitor() throws StandardException {
        _tagStack = new ArrayList<String>();
        _visitedNodes = new HashSet<Visitable>();
        // done as long as setters called
    }

    @Override
    public void teardownVisitor() throws StandardException {
        // client holing ref to connection so his responsibility
    }

    private static Map<Integer,String> phaseMap = new HashMap<Integer, String>(3);
    static {
        phaseMap.put(AFTER_PARSE,"PARSE PHASE");
        phaseMap.put(AFTER_BIND,"BIND PHASE");
        phaseMap.put(AFTER_OPTIMIZE,"OPTIMIZE PHASE");
    }
    @Override
    public void begin(String statementText, int phase) throws StandardException {
        println("======================= "+phaseMap.get(phase)+" (columns 1-based) ==========================");
        println(XML_BOILERPLATE);
        beginTag(QUERY_TREE, null);
        writeTextElement(QUERY_TEXT, null, statementText);
    }

    @Override
    public void end(int phase) throws StandardException {
        endTag();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TreeWalkerConfig Interface
    //
    ///////////////////////////////////////////////////////////////////////////////////

    @Override
    public void setOutputStream(PrintStream out) {
        this._printStream = out;
    }

    @Override
    public void setConnection(Connection connection) {
        this._connection = connection;
    }

    @Override
    public void setStopAfterParse() {
        System.setProperty(DERBY_DEBUG_SETTING, STOP_AFTER_PARSING);
    }

    @Override
    public void setContinueAfterParse() {
        System.setProperty(DERBY_DEBUG_SETTING, CONTINUE_AFTER_PARSE);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // INNER CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Attribute descriptor.
     * </p>
     */
    public static final class Attr {
        public String name;
        public String value;

        public Attr(String name, String value) {
            this.name = name;
            this.value = value;
        }
    }
}