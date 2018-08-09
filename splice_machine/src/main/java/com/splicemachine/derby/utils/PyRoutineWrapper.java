package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.pyprocedure.PyCodeUtil;
import com.splicemachine.db.impl.sql.pyprocedure.PyInterpreterPool;
import com.splicemachine.db.impl.sql.pyprocedure.PyStoredProcedureResultSetFactory;
import org.python.core.PyFunction;
import org.python.core.PyLong;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import java.math.BigInteger;
import java.sql.ResultSet;

public class PyRoutineWrapper {
    /**
     * A wrapper static method for Python Stored Procedure
     *
     * @param args if the registered stored procedure has n parameters, the args[0] to args[n-1] are these parameters.
     * The args[n] must be a String which contains the Python script. The rest of elements in the args are ResultSet[].
     * In the current test version only allows one ResultSet[]
     */
    public static void pyProcedureWrapper(Object... args)
            throws Exception
    {
        PyInterpreterPool pool = null;
        PythonInterpreter interpreter = null;
        String setFacFuncName = "setFactory";
        String funcName = "execute";

        try {
            int pyScriptIdx;
            byte[] compiledCode;
            int nargs = args.length;
            Integer rsDelim = null;


            for(int i = 0; i < nargs; ++i){
                if(args[i] instanceof ResultSet[]){
                    rsDelim = i;
                    break;
                }
            }

            // set pyScript
            if(rsDelim == null){
                pyScriptIdx = nargs - 1;
            }
            else{
                pyScriptIdx = rsDelim - 1;
            }
            compiledCode = (byte[]) args[pyScriptIdx];

            // set the Object[] to pass into Jython code
            int j = 0;
            Object[] pyArgs;
            if(nargs - 1 ==0){
                pyArgs = null;
            }
            else{
                pyArgs = new Object[nargs - 1];
            }
            for(int i = 0; i < nargs; ++i){
                if(i != pyScriptIdx){
                    pyArgs[j] = args[i];
                    j++;
                }
            }

            pool = PyInterpreterPool.getInstance();
            interpreter = pool.acquire();
            PyCodeUtil.exec(compiledCode, interpreter);
            // add global variable factory, so that the user can use it to construct JDBC ResultSet
            Object[] factoryArg = {new PyStoredProcedureResultSetFactory()};
            PyFunction addFacFunc = interpreter.get(setFacFuncName, PyFunction.class);
            addFacFunc._jcall(factoryArg);

            // execute the user defined function, the user needs to fill the ResultSet himself,
            // just like the original Java Stored Procedure
            PyFunction userFunc = interpreter.get(funcName, PyFunction.class);
            if(pyArgs == null){
                userFunc.__call__();
            }else{
                userFunc._jcall(pyArgs);
            }
        }
        catch (Exception e){
            throw StandardException.plainWrapException(e);
        }
        finally{
            if(pool != null && interpreter != null){
                pool.release(interpreter);
            }
        }
    }

    /**
     * A wrapper static method for Python user-defined function
     *
     * @param args if the registered stored procedure has n parameters, the args[0] to args[n-1] are these parameters.
     * The args[n] must be a String which contains the Python script.
     */
    public static Object pyFunctionWrapper(Object... args)
            throws Exception
    {
        PyInterpreterPool pool = null;
        PythonInterpreter interpreter = null;
        String setFacFuncName = "setFactory";
        String funcName = "execute";
        PyObject pyResult = null;
        Object javaResult = null;

        try {
            byte[] compiledCode;
            int nargs = args.length;
            int pyScriptIdx = args.length - 1;

            // set pyScript
            compiledCode = (byte[]) args[pyScriptIdx];

            // set the Object[] to pass in
            Object[] pyArgs;
            if(nargs - 1 ==0){
                pyArgs = null;
            }
            else{
                pyArgs = new Object[nargs - 1];
                System.arraycopy(args, 0, pyArgs, 0, nargs - 1);
            }

            pool = PyInterpreterPool.getInstance();
            interpreter = pool.acquire();
            PyCodeUtil.exec(compiledCode, interpreter);
            // add global variable factory, so that the user can use it to construct JDBC ResultSet
            Object[] factoryArg = {new PyStoredProcedureResultSetFactory()};
            PyFunction addFacFunc = interpreter.get(setFacFuncName, PyFunction.class);
            addFacFunc._jcall(factoryArg);

            // execute the user defined function, the user needs to fill the ResultSet himself,
            // just like the original Java Stored Procedure
            PyFunction userFunc = interpreter.get(funcName, PyFunction.class);
            if(pyArgs == null){
                pyResult = userFunc.__call__();
            }else{
                pyResult = userFunc._jcall(pyArgs);
            }
            javaResult = pyResult.__tojava__(Object.class);
            if(pyResult instanceof PyLong){
                // When the result has type PyLong, the result's corresponding
                // sql type should be BigInt.
                javaResult = ((BigInteger) javaResult).longValue();
            }
        }
        catch (Exception e){
            throw StandardException.plainWrapException(e);
        }
        finally{
            if(pool != null && interpreter != null){
                pool.release(interpreter);
            }

            return javaResult;
        }
    }
}
