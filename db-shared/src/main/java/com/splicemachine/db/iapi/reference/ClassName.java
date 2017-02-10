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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */


package com.splicemachine.db.iapi.reference;


/**
	List of strings representing class names, which are typically found
    for classes with implement the Formatable interface.
    These strings are removed from the code to separate them from the
    strings which need to be internationalized. It also reduces footprint.
    <P>
	This class has no methods, all it contains are String's which by default
	are public, static and final since they are declared in an interface.
*/

public interface ClassName
{

	String STORE_CONGLOMDIR =
		"com.splicemachine.db.impl.store.access.ConglomerateDirectory";

	String STORE_PCXENA =
		"com.splicemachine.db.impl.store.access.PC_XenaVersion";


	String DataValueFactory = "com.splicemachine.db.iapi.types.DataValueFactory";
	String DataValueDescriptor = "com.splicemachine.db.iapi.types.DataValueDescriptor";

	String BooleanDataValue = "com.splicemachine.db.iapi.types.BooleanDataValue";

 	String BitDataValue = "com.splicemachine.db.iapi.types.BitDataValue";
	String StringDataValue = "com.splicemachine.db.iapi.types.StringDataValue";
	String DateTimeDataValue = "com.splicemachine.db.iapi.types.DateTimeDataValue";
	String NumberDataValue = "com.splicemachine.db.iapi.types.NumberDataValue";
	String RefDataValue = "com.splicemachine.db.iapi.types.RefDataValue";
	String UserDataValue = "com.splicemachine.db.iapi.types.UserDataValue";
    String UserType = "com.splicemachine.db.iapi.types.UserType";
	String ConcatableDataValue  = "com.splicemachine.db.iapi.types.ConcatableDataValue";
	String XMLDataValue  = "com.splicemachine.db.iapi.types.XMLDataValue";

	String FormatableBitSet = "com.splicemachine.db.iapi.services.io.FormatableBitSet";

	String BaseActivation = "com.splicemachine.db.impl.sql.execute.BaseActivation";
	String BaseExpressionActivation = "com.splicemachine.db.impl.sql.execute.BaseExpressionActivation";

	String CursorActivation = "com.splicemachine.db.impl.sql.execute.CursorActivation";

	String Row = "com.splicemachine.db.iapi.sql.Row";
	String Qualifier = "com.splicemachine.db.iapi.store.access.Qualifier";

	String RunTimeStatistics = "com.splicemachine.db.iapi.sql.execute.RunTimeStatistics";

	String Storable = "com.splicemachine.db.iapi.services.io.Storable";
	String StandardException = "com.splicemachine.db.iapi.error.StandardException";

	String LanguageConnectionContext = "com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext";
	String ConstantAction = "com.splicemachine.db.iapi.sql.execute.ConstantAction";
	String DataDictionary = "com.splicemachine.db.iapi.sql.dictionary.DataDictionary";

	String CursorResultSet = "com.splicemachine.db.iapi.sql.execute.CursorResultSet";

	String ExecIndexRow = "com.splicemachine.db.iapi.sql.execute.ExecIndexRow";

	String ExecPreparedStatement = "com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement";

	String ExecRow = "com.splicemachine.db.iapi.sql.execute.ExecRow";
	String Activation = "com.splicemachine.db.iapi.sql.Activation";

	String ResultSet = "com.splicemachine.db.iapi.sql.ResultSet";

	String FileMonitor = "com.splicemachine.db.impl.services.monitor.FileMonitor";

	String GeneratedClass = "com.splicemachine.db.iapi.services.loader.GeneratedClass";
	String GeneratedMethod = "com.splicemachine.db.iapi.services.loader.GeneratedMethod";
	String GeneratedByteCode = "com.splicemachine.db.iapi.services.loader.GeneratedByteCode";

	String Context = "com.splicemachine.db.iapi.services.context.Context";

	String NoPutResultSet = "com.splicemachine.db.iapi.sql.execute.NoPutResultSet";

	String ResultSetFactory = "com.splicemachine.db.iapi.sql.execute.ResultSetFactory";
	String RowFactory = "com.splicemachine.db.iapi.sql.execute.RowFactory";

	String RowLocation = "com.splicemachine.db.iapi.types.RowLocation";

	String VariableSizeDataValue = "com.splicemachine.db.iapi.types.VariableSizeDataValue";
	String ParameterValueSet = "com.splicemachine.db.iapi.sql.ParameterValueSet";


	String CurrentDatetime = "com.splicemachine.db.impl.sql.execute.CurrentDatetime";

	String MaxMinAggregator = "com.splicemachine.db.impl.sql.execute.MaxMinAggregator";
	String SumAggregator = "com.splicemachine.db.impl.sql.execute.SumAggregator";
	String CountAggregator = "com.splicemachine.db.impl.sql.execute.CountAggregator";
	String AvgAggregator = "com.splicemachine.db.impl.sql.execute.AvgAggregator";
	String UserDefinedAggregator = "com.splicemachine.db.impl.sql.execute.UserDefinedAggregator";
    String RowNumberFunction = "com.splicemachine.db.impl.sql.execute.RowNumberFunction";
    String DenseRankFunction = "com.splicemachine.db.impl.sql.execute.DenseRankFunction";
    String RankFunction = "com.splicemachine.db.impl.sql.execute.RankFunction";
    String FirstLastValueFunction = "com.splicemachine.db.impl.sql.execute.FirstLastValueFunction";
    String LeadLagFunction = "com.splicemachine.db.impl.sql.execute.LeadLagFunction";
    // TODO: There's no need to reference spliceengine functions and all the baggage they bring in Derby. See impls of RowNumberFunction, RankFunction and DenseRankFunction
    String WindowMaxMinAggregator = "com.splicemachine.derby.impl.sql.execute.operations.window.function.MaxMinAggregator";
    String WindowSumAggregator = "com.splicemachine.derby.impl.sql.execute.operations.window.function.SumAggregator";
    String WindowAvgAggregator = "com.splicemachine.derby.impl.sql.execute.operations.window.function.AvgAggregator";
    String WindowCountAggregator = "com.splicemachine.derby.impl.sql.execute.operations.window.function.CountAggregator";

	String ExecutionFactory = "com.splicemachine.db.iapi.sql.execute.ExecutionFactory";
	String LanguageFactory ="com.splicemachine.db.iapi.sql.LanguageFactory";
	String ParameterValueSetFactory ="com.splicemachine.db.iapi.sql.ParameterValueSetFactory";

	String TriggerNewTransitionRows = "com.splicemachine.db.catalog.TriggerNewTransitionRows";
//	String TriggerOldTransitionRows = "com.splicemachine.db.catalog.TriggerOldTransitionRows";
	String VTICosting = "com.splicemachine.db.vti.VTICosting";

	String Authorizer = "com.splicemachine.db.iapi.sql.conn.Authorizer";
    String UDTBase = "com.splicemachine.db.shared.common.udt.UDTBase";
}
