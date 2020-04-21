package com.splicemachine.db.impl.sql.pyprocedure;

import org.python.compiler.Module;
import org.python.core.CompilerFlags;
import org.python.core.ParserFacade;
import org.python.core.CompileMode;
import org.python.core.PyCode;
import org.python.core.BytecodeLoader;
import org.python.util.PythonInterpreter;

import java.io.ByteArrayOutputStream;
import java.io.StringReader;

/*
 * PyCodeUtil provide static methods for compiling Python script into reusable bytecode
 * and executing the script's bytecode
 * p.s PyCodeUtil is placed under db-engine as it will be used by CreateAliasNode class.
  */
public class PyCodeUtil{

    private static final String NAME = "py$procedure";

    // Does the necessary import and make the connection and make the connection
    private static final String CONNECT = "from com.ziclix.python.sql import zxJDBC\n" +
            "jdbc_url = \"jdbc:default:connection\"\n" +
            "driver = \"com.splicemachine.db.jdbc.ClientDriver\"\n" +
            "global conn\n" +
            "conn = zxJDBC.connect(jdbc_url, None, None, driver)\n";

    // Setup the wrapper method
    private static final String SETUP = "def execute(*args):\n" +
            "    return run(*args)\n" +
            "factory = None\n" +
            "def setFactory(*args):\n" +
            "    global factory\n" +
            "    factory = args[0]\n";
            // Construct function to set the gloabal facotry for type conversion

    public static byte[] compile(String code, boolean sqlAllowed) throws Exception{
        byte[] compiledCode;

    // Only make connection if the PyStored procedure is going to use the connection
        String decoratedCode = sqlAllowed?(CONNECT + SETUP + code):(SETUP + code);

        StringReader codeReader = new StringReader(decoratedCode);
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        try {
            org.python.antlr.base.mod node;
            node = ParserFacade.parse(codeReader, CompileMode.exec, null, new CompilerFlags());
            codeReader.close();
            Module.compile(node, byteStream, NAME, null, true, false, null, org.python.core.imp.NO_MTIME);
            compiledCode = byteStream.toByteArray();
            return compiledCode;
        } catch (Exception e){
            if(e instanceof IllegalArgumentException && e.getMessage() != null && e.getMessage().equals("Cannot create PyString from null!")){
                throw new Exception("Python script cannot compile. This may due to syntax errors!",e);
            }
            else{
                throw e;
            }
        } finally {
            codeReader.close();
        }
    }

    public static void exec(byte[] compiledCode, PythonInterpreter interpreter) throws Exception{
            PyCode pyCode = BytecodeLoader.makeCode(NAME, compiledCode, null);
            interpreter.exec(pyCode);
    }
}
