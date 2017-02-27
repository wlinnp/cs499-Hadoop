package edu.cpp.cs499.A3;

/**
 * @author waiphyo
 *         2/27/17.
 */
public class TopTen {
    private int count = 10;

    private static TopTen INSTANCE = null;

    private TopTen() {
        count = 10;
    }
    public static TopTen getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new TopTen();
        }
        return INSTANCE;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
