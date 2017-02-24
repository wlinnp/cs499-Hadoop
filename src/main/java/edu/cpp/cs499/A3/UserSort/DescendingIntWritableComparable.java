package edu.cpp.cs499.A3.UserSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author waiphyo
 *         2/23/17.
 */
public class DescendingIntWritableComparable<T> extends WritableComparator {
    protected DescendingIntWritableComparable() {
        super(IntWritable.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        IntWritable key1 = (IntWritable) w1;
        IntWritable key2 = (IntWritable) w2;
        return -1 * key1.compareTo(key2);
    }
}
