package com.splicemachine.stats.frequency;

import com.splicemachine.utils.ComparableComparator;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 7/20/15
 */
public class ObjectFrequentElementsTest{

    @Test
    public void testCanFetchAllElementsEmptyCollection() throws Exception{
        FrequentElements<BigDecimal> dfe = ObjectFrequentElements.topK(10,0l,
                Collections.<FrequencyEstimate<BigDecimal>>emptyList(),
                ComparableComparator.<BigDecimal>newComparator());
        Set<? extends FrequencyEstimate<BigDecimal>> frequencyEstimates=dfe.allFrequentElements();
        Assert.assertEquals("Incorrect size!",0,frequencyEstimates.size());
    }
}
