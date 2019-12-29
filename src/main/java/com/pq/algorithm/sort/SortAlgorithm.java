package com.pq.algorithm.sort;

/**
 * @author qiong.peng
 * @Date 2019/9/7
 */
public class SortAlgorithm {

    // 判断传入的数组长度是否大于1
    public static boolean arrayLengthCheck(int[] array) {
        if (array == null)
            throw new NullPointerException();
        return array.length > 1;
    }

    // 冒泡排序
    public static void bubbleSort(int[] array) {
        if (!arrayLengthCheck(array)) return;
        for (int i = 0; i < array.length; i++) {
            // 提前退出冒泡循环的标志位,即一次比较中没有交换任何元素，这个数组就已经是有序的了
            boolean flag = false;
            for (int j = 0; j + 1 < array.length - i; j++) {
                if (array[j] > array[j + 1]) {
                    int max = array[j];
                    array[j] = array[j + 1];
                    array[j + 1] = max;
                    flag = true;
                }
            }
            if (!flag) break;
        }
    }

    // 选择排序
    public static void selectiveSorting(int[] array) {
        if (!arrayLengthCheck(array)) return;
        for (int i = 0; i < array.length; i++) {
            int maxIndex = array.length - i - 1;
            // 最大值所在的索引位置
            int max = maxIndex;
            for (int j = 0; j < array.length - i; j++) {
                if (array[j] > array[max]) {
                    // 跟换最大值所在索引的位置
                    max = j;
                }
            }
            // 如果max的值改变，说明有值比最大值索引位置的值大，则该出的值与最大值索引位置的值交换位置
            if (max != maxIndex) {
                int tmp = array[max];
                array[max] = array[maxIndex];
                array[maxIndex] = tmp;
            }
        }
    }

    // 插入排序
    public static void insertSort(int[] array) {
        if (!arrayLengthCheck(array)) return;
        for (int i = 1; i < array.length; i++) {
            int tmp = array[i];
            for (int j = i - 1; j >= 0; j--) {
                if (array[j] > tmp) {
                    array[j + 1] = array[j];
                    if (j == 0) {
                        array[j] = tmp;
                        break;
                    }
                } else {
                    array[j + 1] = tmp;
                    break;
                }
            }
        }
    }

    // 希尔排序
    public static void shellSort(int[] array) {
        if (!arrayLengthCheck(array)) return;
        int n = array.length;
        for (int gap = n / 2; gap > 0; gap--) {
            for (int i = 0; i < gap; i++) {
                for (int j = i + gap; j < n; j += gap) {
                    // 如果a[j] < a[j-gap]，则寻找a[j]位置，并将后面数据的位置都后移。
                    if (array[j] < array[j - gap]) {
                        int tmp = array[j];
                        int k = j - gap;
                        while (k >= 0 && array[k] > tmp) {
                            array[k + gap] = array[k];
                            k -= gap;
                        }
                        array[k + gap] = tmp;
                    }
                }
            }
        }
    }

    // 快速排序 - 错误的
    public static void quickSort(int[] array) {
        if (!arrayLengthCheck(array)) return;
        int index = 0;
        boolean flag = true;
        int leftIndex = 0;
        int rightIndex = array.length - 1;
        for (int i = 0; i < array.length; i++) {
            if(leftIndex >= rightIndex) break;
            for (int j = 0; j < (flag ? Math.abs(rightIndex - index) : Math.abs(leftIndex - index)); j++) {
                if(flag) {
                    int compareIndex = rightIndex - j;
                    leftIndex++;
                    // 从右往左比较
                    if(array[compareIndex] < array[index]) {
                        int tmp = array[index];
                        array[index] = array[compareIndex];
                        array[compareIndex] = tmp;
                        leftIndex = index;
                        index = compareIndex;
                        flag = false;
                        break;
                    }
                } else {
                    int compareIndex = leftIndex + j;
                    // 从左往右比较
                    rightIndex--;
                    if(array[compareIndex] > array[index]) {
                        int tmp = array[index];
                        array[index] = array[compareIndex];
                        array[compareIndex] = tmp;
                        rightIndex = index;
                        index = compareIndex;
                        flag = true;
                        break;
                    }
                }
            }
        }
        System.out.println("leftIndex = [" + leftIndex + "]");
        System.out.println("rightIndex = [" + rightIndex + "]");
    }

    // 正确的排序算法
    public static void quickSort(int[] a, int l, int r) {
        if (l < r) {
            int i,j,x;

            i = l;
            j = r;
            x = a[i];
            while (i < j) {
                while(i < j && a[j] > x)
                    j--; // 从右向左找第一个小于x的数
                if(i < j)
                    a[i++] = a[j];
                while(i < j && a[i] < x)
                    i++; // 从左向右找第一个大于x的数
                if(i < j)
                    a[j--] = a[i];
            }
            a[i] = x;
            quickSort(a, l, i-1); /* 递归调用 */
            quickSort(a, i+1, r); /* 递归调用 */
        }
    }

    public static void main(String[] args) {
        int[] array = new int[]{78, 12, 46, 12, 93, 9, 32, 22, 70};
        long startTime = System.currentTimeMillis();
        System.out.println("startTime = [" + startTime + "]");
        // 冒泡排序
//        bubbleSort(array);
        // 选择排序
//        selectiveSorting(array);
        // 插入排序
//        insertSort(array);
        // 希尔排序
//        shellSort(array);
        // 快速排序
        quickSort(array);
//        quickSort(array, 0, array.length - 1);
        long endTime = System.currentTimeMillis();
        System.out.println("startTime = [" + endTime + "]");
        System.out.println("spendTime = [" + (endTime - startTime) + "]ms");
        for (int arr : array) {
            System.out.println(arr);
        }
    }
}
