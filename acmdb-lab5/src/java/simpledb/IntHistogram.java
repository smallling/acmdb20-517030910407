package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int min;
    private int max;
    private int ntups;
    private int bins[];

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        bins = new int[buckets];
    	this.min = min;
    	this.max = max;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int tmp = (max - min) / bins.length;
        if(tmp == 0)tmp++;
    	int buck = (v - min) / tmp;
    	if(buck >= bins.length)buck = bins.length - 1;
    	bins[buck]++;
    	ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if(op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ) {
            if(op == Predicate.Op.GREATER_THAN) {
                v++;
            }
            if(v <= min)return 1;
            if(v > max)return 0;
            int tmp = (max - min) / bins.length;
            if(tmp == 0)tmp++;
            int buck = (v - min) / tmp;
            if(buck >= bins.length)buck = bins.length - 1;
            int buckmin = buck * tmp + min;
            int buckmax = buckmin + tmp;
            if(buckmax > max)buckmax = max;

            double nowans = 1. * (buckmax - v) / (buckmax - buckmin) * bins[buck] / ntups;
            for(int i = buck + 1; i < bins.length; i++)
                nowans += 1. * bins[i] / ntups;
            return nowans;
        }
        if(op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ) {
            if(op == Predicate.Op.LESS_THAN) {
                v--;
            }
            if(v >= max)return 1;
            if(v < min)return 0;
            int tmp = (max - min) / bins.length;
            if(tmp == 0)tmp++;
            int buck = (v - min) / tmp;
            if(buck >= bins.length)buck = bins.length - 1;
            int buckmin = buck * tmp + min;
            int buckmax = buckmin + tmp;
            if(buckmax > max)buckmax = max;

            double nowans = 1. * (v - buckmin + 1) / (buckmax - buckmin) * bins[buck] / ntups;
            for(int i = 0; i < buck; i++)
                nowans += 1. * bins[i] / ntups;
            return nowans;
        }
        if(op == Predicate.Op.EQUALS || op == Predicate.Op.LIKE) {
            if(v < min || v > max)return 0;
            int tmp = (max - min) / bins.length;
            if(tmp == 0)tmp++;
            int buck = (v - min) / tmp;
            if(buck >= bins.length)buck = bins.length - 1;
            int buckmin = buck * tmp + min;
            int buckmax = buckmin + tmp;
            if(buckmax > max)buckmax = max;
            double nowans = 1. * bins[buck] / (buckmax - buckmin) / ntups;
            return nowans;
        }
        if(op == Predicate.Op.NOT_EQUALS) {
            if(v < min || v > max)return 1;
            int tmp = (max - min) / bins.length;
            if(tmp == 0)tmp++;
            int buck = (v - min) / tmp;
            if(buck >= bins.length)buck = bins.length - 1;
            int buckmin = buck * tmp + min;
            int buckmax = buckmin + tmp;
            if(buckmax > max)buckmax = max;
            double nowans = 1 - 1. * bins[buck] / (buckmax - buckmin) / ntups;
            return nowans;
        }
        return 1.0;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
