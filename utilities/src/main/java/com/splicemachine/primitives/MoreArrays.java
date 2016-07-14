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

package com.splicemachine.primitives;

import java.util.Arrays;

/**
 * @author Scott Fines
 *         Date: 10/22/14
 */
public class MoreArrays {

    public static double median(double[] elements){
        return median(elements,0,elements.length);
    }

    public static double median(double[] elements, int offset, int length){
        int from = offset;
        int to = offset+length;
        int k = offset+length/2;
        while(from < to){
            int r = from;
            int w = to-1;
            double mid = elements[(r+w)>>1];
            while(r<w){
                if(elements[r] >=mid){
                    double tmp = elements[w];
                    elements[w] = elements[r];
                    elements[r] = tmp;
                    w--;
                }else
                    r++;
            }
            if(elements[r]>mid)
                r--;

            if(k<=r)
                to = r;
            else
                from = r+1;
        }
        return elements[k];
    }

    public static long median(long[] elements){
        return median(elements,0,elements.length);
    }

    public static long median(long[] elements, int offset,int length){
        if(length==1)
            return elements[offset];
        int from = offset;
        int to = offset+length;
        int k = elements.length/2;
        while(from < to){
            int r = from;
            int w = to-1;
            long mid = elements[(r+w)>>1];
            while(r<w){
                if(elements[r] >=mid){
                    long tmp = elements[w];
                    elements[w] = elements[r];
                    elements[r] = tmp;
                    w--;
                }else
                    r++;
            }
            if(elements[r]>mid)
                r--;

            if(k<=r)
                to = r;
            else
                from = r+1;
        }
        return elements[k];
    }

    public static long min(long[] elements){
        return min(elements,0,elements.length);
    }

    public static long min(long[] elements ,int offset,int length){
        long min=Long.MAX_VALUE;
        for(int i=offset;i<length;i++){
            if(min>elements[i])
                min = elements[i];
        }
        return min;
    }

    public static int median(int[] elements){
        return median(elements,0,elements.length);
    }

    public static int median(int[] elements, int offset,int length){
        int from = offset;
        int to = offset+length;
        int k = elements.length/2;
        while(from < to){
            int r = from;
            int w = to;
            int mid = elements[(r+w)>>1];
            while(r<w){
                if(elements[r] >=mid){
                    int tmp = elements[w];
                    elements[w] = elements[r];
                    elements[r] = tmp;
                    w--;
                }else
                    r++;
            }
            if(elements[r]>mid)
                r--;

            if(k<=r)
                to = r;
            else
                from = r+1;
        }
        return elements[k];
    }

    public static void unsignedSort(int[] sort, int offset, int length){
        int stop = offset+length;
        if(stop>sort.length)
            stop = sort.length;

        /*
         * Sort in general ascending order first, then move the positives to before the negatives
         * using a cuckoo-style bounce strategy.
         */
        Arrays.sort(sort,offset,stop);

        int p=offset;
        while(p<stop){
            if(sort[p]>=0)
                break;
            p++;
        }
        if(p==offset ||p==stop) return; //we are done, it's already in unsigned order because the signs are all the same

        for(int cS=0,nM=length;nM>0;cS++){
            int displaced = sort[cS];
            int i = cS;
            do{
                i-=p;
                if(i<offset)
                    i+=stop;
                int t = sort[i];
                sort[i] = displaced;
                displaced = t;
                nM--;
            }while(i!=cS);
        }
    }

    public static void unsignedSort(int[] sort){
        unsignedSort(sort,0,sort.length);
    }

    public static void main(String...args) throws Exception{
//        int[] data = new int[]{-1,1,2,3};
//        unsignedSort(data);
//        System.out.println(Arrays.toString(data));
        int[] data = new int[]{-2,-1,2,3};
        unsignedSort(data);
        System.out.println(Arrays.toString(data));
    }
}
