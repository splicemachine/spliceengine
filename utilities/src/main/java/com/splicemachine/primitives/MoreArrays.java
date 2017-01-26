/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
