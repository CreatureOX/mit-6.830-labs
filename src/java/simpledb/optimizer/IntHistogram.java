package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;
    private int min;
    private int max;
    private int width;
    private int nTuples;

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
        // some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.width = (max - min + 1)/buckets > 0?(max - min + 1)/buckets:1;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        if (v < min || v > max) {
            throw new IllegalArgumentException();
        }
        this.buckets[indexOf(v)]++;
        this.nTuples++;
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
        // some code goes here
        double selectivity = 0F;
        int index = indexOf(v);
        int leftBound = min + index * width;
        int rightBound = min + (index + 1) * width;
        switch (op) {
            case EQUALS:
                selectivity = v >= min && v <= max?(1.0 * buckets[index]/width)/nTuples:0.0;
                break;
            case GREATER_THAN:
                if (v < min) {
                    selectivity = 1.0;
                    break;
                }
                if (v > max) {
                    selectivity = 0.0;
                    break;
                }
                selectivity += 1.0 * buckets[index]/nTuples * (rightBound - v)/width;
                for (int i = index + 1;i < buckets.length; i++) {
                    selectivity += 1.0 * buckets[i]/nTuples;
                }
                break;
            case LESS_THAN:
                if (v < min) {
                    selectivity = 0.0;
                    break;
                }
                if (v > max) {
                    selectivity = 1.0;
                    break;
                }
                selectivity += 1.0 * buckets[index]/nTuples * (v - leftBound)/width;
                for (int i = 0;i < index; i++) {
                    selectivity += 1.0 * buckets[i]/nTuples;
                }
                break;
            case NOT_EQUALS:
                selectivity = 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
                break;
            case GREATER_THAN_OR_EQ:
                selectivity = estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
                break;
            case LESS_THAN_OR_EQ:
                selectivity = estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        selectivity = Math.max(0.0, selectivity);
        selectivity = Math.min(1.0, selectivity);
        return selectivity;
    }

    private int indexOf(int v) {
        int index = (v - min)/width;
        return index == buckets.length? buckets.length - 1: index;
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
        double sum = 0F;
        for (int bucket: buckets) {
            sum += bucket;
        }
        return sum/nTuples;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder stringBuilder = new StringBuilder(String.format("min=%s max=%s width=%s nTuples=%s%n", min, max, width, nTuples));
        for (int i=0;i< buckets.length; i++) {
            stringBuilder
                    .append(String.format("[%s, %s) ", min + i * width, min + (i + 1) * width))
                    .append(String.format("contain tuples=%s%n", buckets[i]));
        }
        return stringBuilder.toString();
    }
}
