package com.pq.datastructure.line;

import java.lang.reflect.Array;

/**
 * @author qiong.peng
 * @Date 2019/9/14
 */
public class GeneralArrayQueue<T> {
    private static final int DEFAULT_SIZE = 12;

    private T[] mArray;

    private int count;

    public GeneralArrayQueue(Class<T> type) {
        this(type, DEFAULT_SIZE);
    }

    public GeneralArrayQueue(Class<T> type, int size) {
        this.mArray = (T[]) Array.newInstance(type, size);
        this.count = 0;
    }

    public void add(T val) {
        mArray[count++] = val;
    }

    public T front() {
        return mArray[0];
    }

    public T pop() {
        T ret = mArray[0];
        count--;
        // TODO 删除第一个数据后，将所有后面的数据前移
        // TODO 如果不使用数组而是用链表的话，将不会有这样的问题
        for (int i = 1; i <= count; i++)
            mArray[i - 1] = mArray[i];
        return ret;
    }

    public int size(){
        return count;
    }

}
