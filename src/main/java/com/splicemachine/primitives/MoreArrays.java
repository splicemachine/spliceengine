package com.splicemachine.primitives;

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
}
