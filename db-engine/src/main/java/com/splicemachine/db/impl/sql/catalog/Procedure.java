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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.JDBC30Translation;
import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Procedure {
    private final Arg[] args;
    private final String name;
    private final int numberOutputParameters;
    private final int numResultSets;
    private final short routineSqlControl; //can be anything from RountineAliasInfo.READS_SQL_DATA, etcprivate final boolean isDeterministic;private final com.splicemachine.db.catalog.TypeDescriptor returnType;	public Procedure()	{	}@java.lang.Override
    private final boolean isDeterministic;
    private final TypeDescriptor returnType;
    private final String ownerClass;

    private Procedure(Arg[] args, String name, int numberOutputParameters,
                      int numResultSets, short routineSqlControl,
                      boolean isDeterministic, TypeDescriptor returnType, String ownerClass) {
        this.args = args;
        this.name = name;
        this.numberOutputParameters = numberOutputParameters;
        this.numResultSets = numResultSets;
        this.routineSqlControl = routineSqlControl;
        this.isDeterministic = isDeterministic;
        this.returnType = returnType;
        this.ownerClass = ownerClass;
    }

    public String getName() {
        return name;
    }

    public String getOwnerClass() {
        return ownerClass;
    }

    public Builder toBuilder(){
        return new Builder(name,numberOutputParameters,numResultSets,routineSqlControl,isDeterministic,returnType,ownerClass,args);
    }

    public static Builder newBuilder(){ return new Builder();}

    public boolean equals(Object o) {
        if (null == o) return true;
        if (!(o instanceof Procedure)) return false;

        Procedure procedure = (Procedure) o;

        return name.equals(procedure.name);

    }

//    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public AliasDescriptor createSystemProcedure(UUID schemaId,
                                                  DataDictionary dataDictionary,
                                                  TransactionController tc) throws StandardException {
        int numArgs = args.length;

        int[] argModes = null;
        if(numArgs!=0){
            argModes = new int[numArgs];
            int numInParam = numArgs-numberOutputParameters;
            for(int i=0;i<numInParam;i++)
                argModes[i] = JDBC30Translation.PARAMETER_MODE_IN;
            for(int i=0;i<numberOutputParameters;i++)
                argModes[numInParam+i] = JDBC30Translation.PARAMETER_MODE_OUT;
        }

        String[] parameterNames = new String[args.length];
        TypeDescriptor[] types = new TypeDescriptor[args.length];
        for(int i=0;i< args.length;i++){
            parameterNames[i] = args[i].getName();
            types[i] = args[i].getType();
        }
        RoutineAliasInfo rai = new RoutineAliasInfo(name, "JAVA",numArgs,
                parameterNames,types,argModes,numResultSets,
                RoutineAliasInfo.PS_JAVA,routineSqlControl,isDeterministic,
                false,true,returnType, null);
        UUID routineId = dataDictionary.getUUIDFactory().createUUID();
        AliasDescriptor ads = new AliasDescriptor(
               dataDictionary,
                routineId,
                name,
                schemaId,
                ownerClass,
                returnType==null? AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR: AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR,
                returnType==null? AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR : AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR,
                false,
                rai,
                null);
        dataDictionary.addDescriptor(ads,null,DataDictionary.SYSALIASES_CATALOG_NUM,false,tc,false);

        return ads;
    }

    public static class Builder{
        private List/*<Arg>*/ args;
        private String name;
        private int numberOutputParameters;
        private int numResultSets;
        private short routineSqlControl; //can be anything from RountineAliasInfo.READS_SQL_DATA, etc
        private boolean isDeterministic = true;
        private TypeDescriptor returnType = null;
        private String ownerClass;

        private Builder(String name, int numberOutputParameters,
                        int numResultSets, short routineSqlControl,
                        boolean deterministic,
                        TypeDescriptor returnType, String ownerClass,
                        Arg[] args) {
            this.name = name;
            this.numberOutputParameters = numberOutputParameters;
            this.numResultSets = numResultSets;
            this.routineSqlControl = routineSqlControl;
            this.isDeterministic = deterministic;
            this.returnType = returnType;
            this.ownerClass = ownerClass;
            this.args = Arrays.asList(args);
        }

        private Builder(){
            this.args = new ArrayList/*<Arg>*/();
        }


        public Builder name(String name){
            this.name = name;
            return this;
        }

        public Builder isDeterministic(boolean isDeterministic){
            this.isDeterministic = isDeterministic;
            return this;
        }

        public Builder numOutputParams(int numOutputParams){
            this.numberOutputParameters = numOutputParams;
            return this;
        }

        public Builder numResultSets(int numResultSets){
            this.numResultSets = numResultSets;
            return this;
        }

        public Builder sqlControl(short sqlControl){
            this.routineSqlControl = sqlControl;
            return this;
        }

        public Builder readsSqlData(){
            return sqlControl(RoutineAliasInfo.READS_SQL_DATA);
        }

        public Builder containsSql(){
            return sqlControl(RoutineAliasInfo.CONTAINS_SQL);
        }

        public Builder modifiesSql(){
            return sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA);
        }

        public Builder returnType(TypeDescriptor returnType){
            this.returnType = returnType;
            return this;
        }

        public Builder arg(String name, TypeDescriptor type){
            this.args.add(Arg.newArg(name,type));
            return this;
        }

        public Builder integer(String name){
            this.args.add(Arg.integer(name));
            return this;
        }

        public Builder smallint(String name){
            this.args.add(Arg.smallint(name));
            return this;
        }

        public Builder catalog(String name){
            this.args.add(Arg.catalog(name));
            return this;
        }

        public Builder varchar(String name, int length){
            this.args.add(Arg.varchar(name,length));
            return this;
        }

        public Builder blob(String name){
            this.args.add(Arg.blob(name));
            return this;
        }

        public Builder charType(String name, int length){
            this.args.add(Arg.newArg(name,DataTypeDescriptor.getCatalogType(Types.CHAR,length)));
            return this;
        }

        public Builder bigint(String name) {
            this.args.add(Arg.newArg(name,DataTypeDescriptor.getCatalogType(Types.BIGINT)));
            return this;
        }

        public Builder ownerClass(String ownerClass){
            this.ownerClass = ownerClass;
            return this;
        }

        public Procedure build(){
            assert name !=null;
            assert numberOutputParameters>=0;
            assert numResultSets>=0;
            assert ownerClass !=null;
            assert ((routineSqlControl==RoutineAliasInfo.READS_SQL_DATA)||
                    (routineSqlControl==RoutineAliasInfo.MODIFIES_SQL_DATA)||
                    (routineSqlControl==RoutineAliasInfo.CONTAINS_SQL)||
                    (routineSqlControl==RoutineAliasInfo.NO_SQL));

            Arg[] argsToPut = new Arg[args.size()];
            args.toArray(argsToPut);
            return new Procedure(argsToPut,name,numberOutputParameters,
                    numResultSets,routineSqlControl,isDeterministic,returnType, ownerClass);
        }

    }

    private static class Arg{
        private final String name;
        private final TypeDescriptor type;

        private Arg(String name, TypeDescriptor type) {
            this.name = name;
            this.type = type;
        }

        String getName(){ return this.name; }

        TypeDescriptor getType(){return this.type;}

        public static Arg integer(String name){
            return new Arg(name,TypeDescriptor.INTEGER);
        }

        public static Arg smallint(String name){
            return new Arg(name,TypeDescriptor.SMALLINT);
        }

        public static Arg newArg(String name, TypeDescriptor type){
            return new Arg(name,type);
        }

        public static Arg catalog(String name){
            return new Arg(name, DataDictionary.CATALOG_TYPE_SYSTEM_IDENTIFIER);
        }

        public static Arg varchar(String name, int length){
            return new Arg(name, DataTypeDescriptor.getCatalogType(Types.VARCHAR, length));
        }
        public static Arg blob(String name){
            return new Arg(name, DataTypeDescriptor.getCatalogType(Types.BLOB));
        }

        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Arg)) return false;

            Arg that = (Arg) o;

            return name.equals(that.name) && type.equals(that.type);

        }

        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + type.hashCode();
            return result;
        }
    }
}
