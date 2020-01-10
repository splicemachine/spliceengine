/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by jleach on 8/8/15.
 */
@Category(ArchitectureIndependent.class)
public class ScanCostFunctionTest {
    private static List<SelectivityHolder>[] baseOnly = new List[10];
    private static List<SelectivityHolder>[] baseAndBaseFilter = new List[10];
    private static List<SelectivityHolder>[] allThree = new List[10];
    static {
        List<SelectivityHolder> entry3B = new LinkedList<>();
        List<SelectivityHolder> entry5B = new LinkedList<>();
        List<SelectivityHolder> entry7B = new LinkedList<>();
        entry3B.add(new ConstantSelectivity(0.5d,3, QualifierPhase.BASE));
        entry5B.add(new ConstantSelectivity(0.25d,5, QualifierPhase.BASE));
        entry7B.add(new ConstantSelectivity(0.75d,7, QualifierPhase.BASE));
        entry7B.add(new ConstantSelectivity(0.125d,7, QualifierPhase.BASE));
        baseOnly[3] = entry3B;
        baseOnly[5] = entry5B;
        baseOnly[7] = entry7B;
        List<SelectivityHolder> entry3BBF = new LinkedList<>();
        List<SelectivityHolder> entry5BBF = new LinkedList<>();
        List<SelectivityHolder> entry7BBF = new LinkedList<>();
        entry3BBF.add(new ConstantSelectivity(0.5d,3, QualifierPhase.FILTER_BASE));
        entry5BBF.add(new ConstantSelectivity(0.25d,5, QualifierPhase.BASE));
        entry7BBF.add(new ConstantSelectivity(0.75d,7, QualifierPhase.FILTER_BASE));
        entry7BBF.add(new ConstantSelectivity(0.125d,7, QualifierPhase.BASE));
        baseAndBaseFilter[3] = entry3BBF;
        baseAndBaseFilter[5] = entry5BBF;
        baseAndBaseFilter[7] = entry7BBF;
    }

    @Test
    public void testEmptyPredicatesEqualSelectivityOne() throws Exception {
        List<SelectivityHolder>[] selectivityHolder = new List[6];
        Assert.assertTrue("empty predicates do not have selectivity 1.0",ScanCostFunction.computeTotalSelectivity(selectivityHolder) == 1.0d);
    }

    @Test
    public void testComputeAllSelectivity() throws Exception {
        Assert.assertEquals("TotalSelectivity For All Base Qualifiers incorrect",
                0.125d*Math.sqrt(0.25d)*Math.sqrt(Math.sqrt(0.5d))*Math.sqrt(Math.sqrt(Math.sqrt(.75d))),
                        ScanCostFunction.computeTotalSelectivity(baseOnly),
                        0.0d
                );
    }

    @Test
    public void testComputeBaseSelectivity() throws Exception {
        Assert.assertEquals("TotalSelectivity For All Base Qualifiers incorrect",
                0.125d*Math.sqrt(0.25d)*Math.sqrt(Math.sqrt(0.5d))*Math.sqrt(Math.sqrt(Math.sqrt(.75d))),
                ScanCostFunction.computePhaseSelectivity(baseOnly,QualifierPhase.BASE),
                0.0d
        );
    }

    @Test
    public void testComputeBaseOnlySelectivity() throws Exception {
        Assert.assertEquals("TotalSelectivity For All Base Qualifiers incorrect",
                0.125d*Math.sqrt(0.25d),
                ScanCostFunction.computePhaseSelectivity(baseAndBaseFilter,QualifierPhase.BASE),
                0.0d
        );
    }

}
