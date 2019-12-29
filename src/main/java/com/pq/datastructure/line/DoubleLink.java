package com.pq.datastructure.line;

/**
 * 双向链表
 *
 * @author qiong.peng
 * @Date 2019/9/4
 */
public class DoubleLink<T> {

    // 表头，表头为空不存数据
    private DNode<T> mHead;
    private int mCount;

    /**
     * 双向链表的节点
     *
     * @param <T>
     */
    private class DNode<T> {
        // 节点数据
        public T value;
        // 节点的前一个节点
        public DNode<T> prev;
        // 节点的后一个节点
        public DNode<T> next;

        public DNode(T value, DNode<T> prev, DNode<T> next) {
            this.value = value;
            this.prev = prev;
            this.next = next;
        }
    }

    public DoubleLink() {
        // 创建表头
        mHead = new DNode<T>(null, null, null);
        mHead.prev = mHead.next = mHead;
        mCount = 0;
    }

    public DNode<T> getmHead() {
        return mHead;
    }

    // 返回节点数目
    public int size() {
        return mCount;
    }

    // 返回链表是否为空
    public boolean isEmpty() {
        return mCount == 0;
    }

    // 获取第index位置的节点
    public DNode<T> getNode(int index) {
        if (index < 0 || index >= mCount)
            throw new IndexOutOfBoundsException();

        // 正向查找
        if (index <= mCount / 2) {
            DNode<T> node = mHead.next;
            for (int i = 0; i < index; i++)
                node = node.next;
            return node;
        }

        // 反向查找
        DNode<T> node = mHead.prev;
        int rindex = mCount - index - 1;
        for (int j = 0; j < rindex; j++) {
            node = node.prev;
        }
        return node;
    }

    // 获取第index位置的节点的值
    public T get(int i) {
        return getNode(i).value;
    }

    // 获取第1个节点的值
    public T getFirst() {
        return getNode(0).value;
    }

    // 获取最后一个节点的值
    public T getLast() {
        return getNode(mCount - 1).value;
    }

    // 将节点插入到第index位置之前
    public void insert(int index, T t) {
        if (index == 0) {
            DNode<T> node = new DNode<T>(t, mHead, mHead.next);
            mHead.next.prev = node;
            mHead.next = node;
            mCount++;
            return ;
        }

        DNode<T> node = getNode(index);
        DNode<T> prevNode = node.prev;
        DNode<T> newNode = new DNode<T>(t, prevNode, node);

        prevNode.next = newNode;
        node.prev = newNode;
        mCount++;
    }

    // 在最后一个节点中插入节点
    public void insertLast(T t) {
        DNode<T> node = new DNode<T>(t, mHead.prev, mHead);
        mHead.prev.next = node;
        mHead.prev = node;
        mCount++;
    }

    public void del(int index) {
        DNode<T> node = getNode(index);
        DNode<T> tmpNode = node.prev;
        node.prev.next = node.next;
        node.next.prev =tmpNode;
        // 垃圾回收器回收被删除的节点
        node = null;
        mCount--;
    }


    public static void main(String[] args) {
        DoubleLink<Integer> dl = new DoubleLink<Integer>();
        for (int i = 0; i < 4; i++) {
            dl.insertLast(i);
        }
        for (int i = 0; i < dl.mCount; i++) {
            System.out.println("node " + i + " = [" + dl.get(i) + "]");
        }
    }
}
