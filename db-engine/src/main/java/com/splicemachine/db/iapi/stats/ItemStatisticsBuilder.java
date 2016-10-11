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
package com.splicemachine.db.iapi.stats;

import com.yahoo.sketches.quantiles.ItemsUnion;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
import java.util.Comparator;

/**
 *
 */
public class ItemStatisticsBuilder<I extends Comparator> {
    protected Union thetaSketchUnion;
    protected ItemsUnion quantilesSketchUnion;
    protected com.yahoo.sketches.frequencies.ItemsSketch<I> frequenciesSketch;
    protected long nullCount = 0l;

    public ItemStatisticsBuilder() {

    }

    public static ItemStatisticsBuilder instance() {
        return new ItemStatisticsBuilder();
    }

    public ItemStatisticsBuilder addThetaSketch(Sketch sketch) {
        if (thetaSketchUnion==null)
            thetaSketchUnion = Sketches.setOperationBuilder().buildUnion();
        thetaSketchUnion.update(sketch);
        return this;
    }

    public ItemStatisticsBuilder addQuantilesSketch(com.yahoo.sketches.quantiles.ItemsSketch<I> sketch, I item) {
        if (quantilesSketchUnion==null)
            quantilesSketchUnion = ItemsUnion.getInstance(item);
        quantilesSketchUnion.update(sketch);
        return this;
    }

    public ItemStatisticsBuilder addFrequenciesSketch(com.yahoo.sketches.frequencies.ItemsSketch<I> sketch) {
        if (frequenciesSketch==null)
            frequenciesSketch =  new com.yahoo.sketches.frequencies.ItemsSketch(1024);
        frequenciesSketch.merge(sketch);
        return this;
    }

    public ItemStatisticsBuilder addNullCount(long nullCount) {
        this.nullCount += nullCount;
        return this;
    }

    public com.yahoo.sketches.frequencies.ItemsSketch<I> buildFrequenciesSketch() {
        return frequenciesSketch;
    }

    public com.yahoo.sketches.quantiles.ItemsSketch<I> buildQuantilesSketch() {
        return quantilesSketchUnion.getResult();
    }

    public Sketch buildThetaSketch() {
        return thetaSketchUnion.getResult();
    }

}
