package com.splicemachine.derby.utils;

import com.splicemachine.db.impl.sql.pyprocedure.PyCodeUtil;
import com.splicemachine.db.impl.sql.pyprocedure.PyInterpreterPool;
import com.splicemachine.db.impl.sql.pyprocedure.PyStoredProcedureResultSetFactory;
import org.python.core.PyFunction;
import org.python.util.PythonInterpreter;

import java.sql.ResultSet;

public class PyRoutineWrapper {
    /**
     * A wrapper static method for Python Stored Procedure
     *
     * @param args if the registered stored procedure has n parameters, the args[0] to args[n-1] are these parameters.
     * The args[n] must be a String which contains the Python script. The rest of elements in the args are ResultSet[].
     * In the current test version only allows one ResultSet[]
     */
    public static void PyProcedureWrapper(Object... args)
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
                if(nargs == 0 || !(args[nargs-1] instanceof byte[])){
                    // Might need to raise exception as compiled Bytecode does not exist
                }
                pyScriptIdx = nargs - 1;
            }
            else{
                if(rsDelim-1 < 0 ||!(args[rsDelim - 1] instanceof byte[])){
                    // Might need to raise exception as compiled Bytecode does not exist
                }
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
        }finally{
            if(pool != null && interpreter != null){
                pool.release(interpreter);
            }
        }
    }
}
