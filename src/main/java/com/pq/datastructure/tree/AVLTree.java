package com.pq.datastructure.tree;

/**
 * @author qiong.peng
 * @Date 2019/9/21
 */
public class AVLTree<T extends Comparable<T>> {

    private AVLTreeNode<T> mRoot;

    /**
     * 树的节点类
     * @param <T>
     */
    public class AVLTreeNode<T extends Comparable<T>>{
        // TODO 此处是否需要节点的父节点
        public T key;
        public AVLTreeNode<T> left;
        public AVLTreeNode<T> right;
        public int height;

        public AVLTreeNode(T key, AVLTreeNode<T> left, AVLTreeNode<T> right) {
            this.key = key;
            this.left = left;
            this.right = right;
            this.height = 0;
        }
    }

    // 获取树的高度
    private int height(AVLTreeNode<T> tree) {
        if (tree == null) {
            return 0;
        }
        return tree.height;
    }

    public int height() {
        return height(mRoot);
    }

    /**
     * LL:左左对应的情况(左单旋转)
     *
     * @param k2
     * @return 旋转后的根节点
     */
    private AVLTreeNode<T> leftLeftRotation(AVLTreeNode<T> k2) {
        AVLTreeNode<T> k1;

        // 因为没有根节点，所以直接将k2的左节点指给k1
        k1 = k2.left;
        // k1替换k2的位置
        k2.left = k1.right;
        k1.right = k2;

        k2.height = Math.max(height(k2.left), height(k2.right)) + 1;
        k1.height = Math.max(height(k1.left), k2.height) + 1;

        return k1;
    }

    /**
     * RR:右右对应的情况(右单旋转)
     * @param k1
     * @return
     */
    private AVLTreeNode<T> rightRightRotation(AVLTreeNode<T> k1) {
        AVLTreeNode<T> k2;

        k2 = k1.right;
        k1.right = k2.left;
        k2.left = k1;

        k1.height = Math.max(height(k1.left), height(k1.right)) + 1;
        k2.height = Math.max(k1.height, height(k2.right)) + 1;

        return k2;
    }

    /**
     * LR：左右对应的情况
     * @param k3
     * @return
     */
    private AVLTreeNode<T> leftRightRotation(AVLTreeNode<T> k3) {
        // 先对节点的左节点做RR旋转
        k3.left = rightRightRotation(k3.left);
        // 然后对自己做LL旋转
        return leftLeftRotation(k3);
    }

    /**
     * RL：右左对应的情况
     * @param k3
     * @return
     */
    private AVLTreeNode<T> rightLeftRotation(AVLTreeNode<T> k3) {
        // 先对节点的左节点做RR旋转
        k3.left = leftLeftRotation(k3.right);
        // 然后对自己做LL旋转
        return rightRightRotation(k3);
    }

    // LL, LR, RL, RR 只有在树的结构发生变化的时候才需要旋转，如插入/删除节点

    private AVLTreeNode<T> insert(AVLTreeNode<T> tree, T key) {
        if (tree == null) {
            tree = new AVLTreeNode<>(key, null, null);
        } else {
            int cmp = key.compareTo(tree.key);
            // 判断新增的阶段是插入左子树还是右子树
            if (cmp < 0) {
                // 通过迭代插入后，判断是否需要旋转，实现自平衡
                tree.left = insert(tree.left, key);
                // 判断AVL树是否失去平衡
                if (Math.abs(height(tree.left) - height(tree.right)) > 1) {
                    // 失去平衡, 如何判断需要使用哪一种旋转实现平衡呢
                    if (key.compareTo(tree.left.key) < 0) {
                        tree = leftLeftRotation(tree);
                    } else {
                        tree = leftRightRotation(tree);
                    }
                }
            } else if (cmp > 0) {
                tree.right = insert(tree.right, key);
                if (Math.abs(height(tree.right) - height(tree.left)) > 1) {
                    if (key.compareTo(tree.right.key) > 0) {
                        tree = rightRightRotation(tree);
                    } else {
                        tree = rightLeftRotation(tree);
                    }
                }
            } else {
                throw new RuntimeException("插入的节点已存在");
            }
        }
        tree.height = Math.max(height(tree.left), height(tree.right));
        return tree;
    }

    // 查找最大的节点
    private AVLTreeNode<T> maxMum(AVLTreeNode<T> tree) {
        if (tree == null) {
            return null;
        }
        while (tree.right != null) {
            tree = tree.right;
        }
        return tree;
    }

    public T maxMum() {
        AVLTreeNode<T> p = maxMum(mRoot);
        if (p != null) {
            return p.key;
        }
        return null;
    }

    // 查找最大的节点
    private AVLTreeNode<T> minMum(AVLTreeNode<T> tree) {
        if (tree == null) {
            return null;
        }
        while (tree.left != null) {
            tree = tree.left;
        }
        return tree;
    }

    public T minMum() {
        AVLTreeNode<T> p = minMum(mRoot);
        if (p != null) {
            return p.key;
        }
        return null;
    }

    /**
     * 往AVL书中插入节点
     * @param key
     */
    public void insert(T key) {
        mRoot = insert(mRoot, key);
    }

    private AVLTreeNode<T> remove(AVLTreeNode<T> tree, AVLTreeNode<T> z) {
        if (tree == null || z == null) {
            return null;
        }
        int cmp = z.key.compareTo(tree.key);
        if (cmp < 0) {
            // 要删除的节点在tree的左子树，故删除后，可能会发生旋转，故需要重新指定tree的左子树
            tree.left = remove(tree.left, z);
            if (Math.abs(height(tree.right) - height(tree.left)) > 1) {
                AVLTreeNode<T> r = tree.right;
                // 因为删除的是tree的左子树的节点，则tree的右子树会失去平衡
                // 这俩判断的是大于，而不是判断两颗子树的高度超过2，因为是根节点的左右子树失衡，而右子树的高度大于左子树，故只能对左子树做旋转平衡右子树
                if (height(r.left) > height(r.right)) {
                    tree = rightLeftRotation(tree);
                } else {
                    tree = rightRightRotation(tree);
                }
            }
        } else if (cmp > 0) {
            tree.right = remove(tree.right, z);
            if (Math.abs(height(tree.left) - height(tree.right)) > 1) {
                AVLTreeNode<T> l = tree.left;
                if (height(l.left) < height(l.right)) {
                    tree = leftRightRotation(tree);
                } else {
                    tree = leftLeftRotation(tree);
                }
            }
        } else {
            if (tree.left != null && tree.right != null) {
                if (height(tree.left) > height(tree.right)) {
                    // 如果tree的左子树比右子树高；
                    // 则(01)找出tree的左子树中的最大节点
                    //   (02)将该最大节点的值赋值给tree。
                    //   (03)删除该最大节点。
                    // 这类似于用"tree的左子树中最大节点"做"tree"的替身；
                    // 采用这种方式的好处是：删除"tree的左子树中最大节点"之后，AVL树仍然是平衡的。
                    AVLTreeNode<T> max = maxMum(tree.left);
                    tree.key = max.key;
                    tree.left = remove(tree.left, max);
                } else {
                    // 如果tree的左子树不比右子树高(即它们相等，或右子树比左子树高1)
                    // 则(01)找出tree的右子树中的最小节点
                    //   (02)将该最小节点的值赋值给tree。
                    //   (03)删除该最小节点。
                    // 这类似于用"tree的右子树中最小节点"做"tree"的替身；
                    // 采用这种方式的好处是：删除"tree的右子树中最小节点"之后，AVL树仍然是平衡的。
                    AVLTreeNode<T> min = minMum(tree.right);
                    tree.key = min.key;
                    tree.right = remove(tree.right, min);
                }
            } else {
                AVLTreeNode<T> tmp = tree;
                // 这个地方是让节点的引用指向他左节点或有节点的几点所在地，一次达到节点替换的目的
                tree = (tree.left!=null) ? tree.left : tree.right;
                tmp = null;
            }
        }
        return tree;
    }

    // 移出节点
    public void remove(T key) {
        AVLTreeNode<T> z = search(key);
        if(z != null) {
            mRoot = remove(mRoot, z);
        }
    }

    // 查找节点
    private AVLTreeNode<T> search(AVLTreeNode<T> tree, T key) {
        if (tree == null) {
            return null;
        }
        int cmp = tree.key.compareTo(key);
        if (cmp < 0) {
            return search(tree.right, key);
        } else if (cmp > 0) {
            return search(tree.left, key);
        } else {
            return tree;
        }
    }

    public AVLTreeNode<T> search(T key) {
        return search(mRoot, key);
    }

    private void preOrder(AVLTreeNode<T> tree) {
        if (tree != null) {
            System.out.print(tree.key + "   ");
            preOrder(tree.left);
            preOrder(tree.right);
        }
    }

    // 前序遍历
    public void preOrder() {
        preOrder(mRoot);
    }

    private void inOrder(AVLTreeNode<T> tree) {
        if (tree != null) {
            preOrder(tree.left);
            System.out.print(tree.key + "   ");
            preOrder(tree.right);
        }
    }

    // 中序遍历
    public void inOrder() {
        inOrder(mRoot);
    }

    private void postOrder(AVLTreeNode<T> tree) {
        if (tree != null) {
            preOrder(tree.left);
            preOrder(tree.right);
            System.out.print(tree.key + "   ");
        }
    }

    // 后续遍历
    public void postOrder() {
        postOrder(mRoot);
    }

    // 销毁树，就是将他所有的节点指向空
    private void destroy(AVLTreeNode<T> tree) {
        if (tree==null)
            return ;

        if (tree.left != null)
            destroy(tree.left);
        if (tree.right != null)
            destroy(tree.right);
        tree = null;
    }

    public void destroy() {
        destroy(mRoot);
    }

    /*
     * 打印"二叉查找树"
     *
     * key        -- 节点的键值
     * direction  --  0，表示该节点是根节点;
     *               -1，表示该节点是它的父结点的左孩子;
     *                1，表示该节点是它的父结点的右孩子。
     */
    private void print(AVLTreeNode<T> tree, T key, int direction) {
        if(tree != null) {
            if(direction==0)    // tree是根节点
                System.out.printf("%2d is root\n", tree.key, key);
            else                // tree是分支节点
                System.out.printf("%2d is %2d's %6s child\n", tree.key, key, direction==1?"right" : "left");

            print(tree.left, tree.key, -1);
            print(tree.right,tree.key,  1);
        }
    }

    public void print() {
        if (mRoot != null)
            print(mRoot, mRoot.key, 0);
    }

    private static int arr[]= {3,2,1,4,5,6,7,16,15,14,13,12,11,10,8,9};

    public static void main(String[] args) {
        int i;
        AVLTree<Integer> tree = new AVLTree<Integer>();

        System.out.printf("== 依次添加: ");
        for(i=0; i<arr.length; i++) {
            System.out.printf("%d ", arr[i]);
            tree.insert(arr[i]);
        }

        System.out.printf("\n== 前序遍历: ");
        tree.preOrder();

        System.out.printf("\n== 中序遍历: ");
        tree.inOrder();

        System.out.printf("\n== 后序遍历: ");
        tree.postOrder();
        System.out.printf("\n");

        System.out.printf("== 高度: %d\n", tree.height());
        System.out.printf("== 最小值: %d\n", tree.minMum());
        System.out.printf("== 最大值: %d\n", tree.minMum());
        System.out.printf("== 树的详细信息: \n");
        tree.print();

        i = 8;
        System.out.printf("\n== 删除根节点: %d", i);
        tree.remove(i);

        System.out.printf("\n== 高度: %d", tree.height());
        System.out.printf("\n== 中序遍历: ");
        tree.inOrder();
        System.out.printf("\n== 树的详细信息: \n");
        tree.print();

        // 销毁二叉树
        tree.destroy();
    }

}
