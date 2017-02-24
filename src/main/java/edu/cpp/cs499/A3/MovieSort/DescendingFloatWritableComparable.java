package edu.cpp.cs499.A3.MovieSort;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author waiphyo
 *         2/23/17.
 */
public class DescendingFloatWritableComparable<T> extends WritableComparator {
    protected DescendingFloatWritableComparable() {
        super(FloatWritable.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        FloatWritable key1 = (FloatWritable) w1;
        FloatWritable key2 = (FloatWritable) w2;
        return -1 * key1.compareTo(key2);
    }
}
