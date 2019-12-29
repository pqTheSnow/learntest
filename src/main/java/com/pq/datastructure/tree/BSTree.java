package com.pq.datastructure.tree;

/**
 * 二叉查找树：节点的左子树所有节点的值都比该节点小，右子树的的所有节点都比该节点大
 * TODO 问题：如果键值相等，如何处理
 * TODO 二叉查找树的查找，插入，删除有点复杂
 * @author qiong.peng
 * @Date 2019/9/16
 */
public class BSTree<T extends Comparable<T>> {

    // TODO 为什么要声明根节点变量
    private BSTNode<T> mRoot; // 根节点

    public class BSTNode<T extends Comparable<T>> {
        // TODO 键值对是否要分开
        T key; // 键值
        BSTNode<T> left; // 左孩子
        BSTNode<T> right; // 右孩子
        BSTNode<T> parent; // 父节点

        public BSTNode(T key, BSTNode<T> left, BSTNode<T> right, BSTNode<T> parent) {
            this.key = key;
            this.left = left;
            this.right = right;
            this.parent = parent;
        }
    }

    // 前序遍历
    private void preOrder(BSTNode<T> tree) {
        if (tree != null) {
            System.out.print(tree.key + "   ");
            // 迭代实现
            preOrder(tree.left);
            preOrder(tree.right);
        }
    }

    public void preOrder() {
        preOrder(mRoot);
    }

    // 中序遍历
    private void inOrder(BSTNode<T> tree) {
        if (tree != null) {
            // 迭代实现
            inOrder(tree.left);
            System.out.print(tree.key + "   ");
            inOrder(tree.right);
        }
    }

    public void inOrder() {
        inOrder(mRoot);
    }

    // 后续遍历
    private void postOrder(BSTNode<T> tree) {
        if (tree != null) {
            // 迭代实现
            postOrder(tree.left);
            postOrder(tree.right);
            System.out.print(tree.key + "   ");
        }
    }

    public void postOrder() {
        postOrder(mRoot);
    }

    // 非迭代搜索，容易理解
    private BSTNode<T> search(BSTNode<T> node, T key) {
        while (node != null) {
            int cmp = node.key.compareTo(key);
            if(cmp < 0) {
                node = node.right;
            } else if (cmp > 0) {
                node = node.left;
            } else {
                return node;
            }
        }
        return null;
    }

    public BSTNode<T> search(T key) {
        return search(mRoot, key);
    }

    // 查找最大的节点
    private BSTNode<T> maxMum(BSTNode<T> tree) {
        if (tree == null) {
            return null;
        }
        while (tree.right != null) {
            tree = tree.right;
        }
        return tree;
    }

    public T maxMum() {
        BSTNode<T> p = maxMum(mRoot);
        if (p != null) {
            return p.key;
        }
        return null;
    }

    // 查找最大的节点
    private BSTNode<T> minMum(BSTNode<T> tree) {
        if (tree == null) {
            return null;
        }
        while (tree.left != null) {
            tree = tree.left;
        }
        return tree;
    }

    public T minMum() {
        BSTNode<T> p = minMum(mRoot);
        if (p != null) {
            return p.key;
        }
        return null;
    }

    // TODO 树的节点一定有前驱或后继节点吗
    // 前驱节点 TODO 节点val值小于该节点val值并且值最大的节点
    // 查找指定节点的前驱节点
    public BSTNode<T> preDecessor(BSTNode<T> x) {
        // 如果x存在左孩子，则"x的前驱结点"为 "以其左孩子为根的子树的最大结点"。
        if (x.left != null) {
            return maxMum(x);
        }

        // 如果x没有左孩子。则x有以下两种可能：
        // (01) x是"一个右孩子"，则"x的前驱结点"为 "它的父结点"。
        // (01) x是"一个左孩子"，则查找"x的最低的父结点，并且该父结点要具有右孩子"，找到的这个"最低的父结点"就是"x的前驱结点"。
        //      o(a)
        //       \
        //        o(b)
        //       /
        //      o(c)
        // c的前驱节点是a
        BSTNode<T> y = x.parent;
        while ((y!=null) && (x==y.left)) {
            x = y;
            y = y.parent;
        }
        return y;
    }

    // 后继节点 TODO 节点val值大于该节点val值并且值最小的节点
    public BSTNode<T> successor(BSTNode<T> x) {
        // 如果x存在右孩子，则"x的后继结点"为 "以其右孩子为根的子树的最小结点"。
        if (x.right != null) {
            return minMum(x);
        }

        // 如果x没有右孩子。则x有以下两种可能：
        // (01) x是"一个左孩子"，则"x的后继结点"为 "它的父结点"。
        // (02) x是"一个右孩子"，则查找"x的最低的父结点，并且该父结点要具有左孩子"，找到的这个"最低的父结点"就是"x的后继结点"。
        BSTNode<T> y = x.parent;
        while ((y!=null) && (x==y.right)) {
            x = y;
            y = y.parent;
        }
        return y;
    }

    // 新增节点
    private void insert(BSTNode<T> z) {
        int cmp;
        // 中间参数
        BSTNode<T> y = null;
        BSTNode<T> x = this.mRoot;

        while (x != null) {
            y = x;
            cmp = z.key.compareTo(y.key);
            if (cmp == 0) {
                throw new RuntimeException("已存在相同的key节点");
            } else if (cmp > 0) {
                x = x.right;
            } else {
                x = x.left;
            }
        }

        // x 为null，说明x在的地方为该插入的地方
        z.parent = y;
        // x为空，则y为空
        if(y == null) {
            this.mRoot = z;
        } else {
            cmp = z.key.compareTo(y.key);
            if (cmp > 0) {
                y.right = z;
            } else {
                y.left = z;
            }
        }
    }

    public void insert(T key) {
        BSTNode<T> z = new BSTNode<>(key, null, null, null);
        insert(z);
    }

    // 返回被删除的节点
    // 事情不是一蹴而就的，分情况弄清楚，在一个一个的整合就可以了
    // 删除节点
    private BSTNode<T> delete(BSTNode<T> z) {
        BSTNode<T> x=null;
        BSTNode<T> y=null;

        if ((z.left == null) || (z.right == null) )
            y = z;
        else
            // 这一步是将z的后继节点替换z，就转换成了上面的那种情况(被删除的几点至少有一个子节点不存在)
            y = successor(z);

        // x标识y不为空的节点，也就是需要替换y的节点
        if (y.left != null)
            x = y.left;
        else
            x = y.right;

        // 如果x存在，则将x的父节点指向y的父节点(y是要被删除的节点，x替换y)
        if (x != null)
            x.parent = y.parent;

        // 如果y没有父节点即y为根节点，替换y的节点直接为根节点节课
        if (y.parent == null)
            this.mRoot = x;
        else if (y == y.parent.left)
            y.parent.left = x;
        else
            y.parent.right = x;

        // 如果y和z相等，表示y最多只有一个子节点，且已被x替换
        // 如果y和z不相等，表示也是子的后继节点，上述操作将y节点删除，现在需要将y节点替换z节点，即可删除z，而y有移动到了z节点位置上
        if (y != z)
            z.key = y.key;

        return y;
    }
}
