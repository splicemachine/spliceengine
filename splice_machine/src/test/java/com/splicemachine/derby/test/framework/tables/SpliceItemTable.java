/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.test.framework.tables;

import com.splicemachine.derby.test.framework.SpliceTableWatcher;

public class SpliceItemTable extends SpliceTableWatcher {
	public static final String TABLE_NAME = "ITEM";
	public static final String CREATE_STRING = "(itm_id INT," +
            "itm_name VARCHAR(128)," +
            "itm_long_desc VARCHAR(32672)," +
            "itm_foreign_name VARCHAR(128)," +
            "itm_url VARCHAR(1024)," +
            "itm_disc_cd VARCHAR(64)," +
            "itm_upc VARCHAR(64)," +
            "itm_warranty VARCHAR(1)," +
            "itm_unit_price FLOAT," +
            "itm_unit_cost FLOAT," +
            "itm_subcat_id INT," +
            "itm_supplier_id INT," +
            "itm_brand_id INT," +
            "itm_name_de VARCHAR(128)," +
            "itm_name_fr VARCHAR(128)," +
            "itm_name_es VARCHAR(128)," +
            "itm_name_it VARCHAR(128)," +
            "itm_name_po VARCHAR(128)," +
            "itm_name_ja VARCHAR(128)," +
            "itm_name_sch VARCHAR(128)," +
            "itm_name_ko VARCHAR(128)," +
            "itm_long_desc_de VARCHAR(32672)," +
            "itm_long_desc_fr VARCHAR(32672)," +
            "itm_long_desc_es VARCHAR(32672)," +
            "itm_long_desc_it VARCHAR(32672)," +
            "itm_long_desc_po VARCHAR(32672)," +
            "itm_itm_long_desc_ja VARCHAR(32672)," +
            "itm_long_desc_sch VARCHAR(32672)," +
            "itm_long_desc_ko VARCHAR(32672))";
	public SpliceItemTable(String schemaName) {
		this(TABLE_NAME,schemaName);
	}
	public SpliceItemTable(String itemName, String schemaName) {
		super(itemName,schemaName,CREATE_STRING);
	}	
	
}
