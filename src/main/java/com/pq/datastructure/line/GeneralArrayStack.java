package com.pq.datastructure.line;


import java.lang.reflect.Array;

/**
 * 写一个数据结构的时候，先判断是否支持泛型
 * 然后判断他有那些属性
 *  比如大小
 * 再然后判断有哪些方法
 *  比如添加元素，删除元素，查找元素
 *
 * 这是一个用数组结构实现的栈
 * @author qiong.peng
 * @Date 2019/9/13
 */
public class GeneralArrayStack<T> {
    // 数组默认大小
    private static final int DEFAULT_SIZE = 12;

    // 栈中实际存放数据的数据结构
    private T[] mArray;

    // 元素的个数
    private int count;

    public GeneralArrayStack(Class<T> type) {
        this(type, DEFAULT_SIZE);
    }

    public GeneralArrayStack(Class<T> type, int size) {
        mArray = (T[]) Array.newInstance(type, size);
        this.count = 0;
    }

    // 往栈中存放数据
    public T push(T val) {
        // TODO 如果栈的大小超出了默认大小解决办法
        mArray[count] = val;
        count++;
        return val;
    }

    // 返回栈顶元素
    public T peek(){
        return mArray[count - 1];
    }

    // 返回栈顶元素，并删除
    public T pop(){
        T val = mArray[count - 1];
        mArray[count - 1] = null;
        count--;
        return val;
    }

    // 返回栈的大小
    public int size(){
        return count;
    }

    // 返回“栈”是否为空
    public boolean isEmpty() {
        return size()==0;
    }

    // 打印“栈”
    public void PrintArrayStack() {
        if (isEmpty()) {
            System.out.printf("stack is Empty\n");
        }

        System.out.printf("stack size()=%d\n", size());

        // 打印栈不是调用对外获取元素的方法，因为对外的方法智能获取到栈顶的方法
        // 打印栈的方法，是自己从内部获取数据，不对外开放
        // 也就是说栈其实是一种只提供部分功能的线性表
        // 他可以提供其他的功能，但是不提供，实现特殊功能，防止误操作
        int i=size()-1;
        while (i>=0) {
            System.out.println(mArray[i]);
            i--;
        }
    }


    public static void main(String[] args) {
        GeneralArrayStack<Integer> stack = new GeneralArrayStack<>(Integer.class);
        stack.push(1);
        stack.push(2);
        stack.push(3);
        stack.push(4);
        for (int i = 0; i < 4; i++) {
            System.out.println("args = [" + stack.pop() + "]");
        }
        System.out.println("size = [" + stack.size() + "]");
    }
}
