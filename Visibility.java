package top.guoziyang.mydb.backend.vm;

import top.guoziyang.mydb.backend.tm.TransactionManager;

public class Visibility {
    
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if(t.level == 0) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true;

        if(tm.isCommitted(xmin)) {
            if(xmax == 0) return true;
            if(xmax != xid) {
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     *  @XMIN 表示创建这个数据版本的事务 ID
     * @SP(Ti) 表示活跃事务集合，即当前正在活跃的事务 ID 集合
     * 当事务 Ti 尝试访问某个数据版本时，它会检查该数据版本的 XMIN 是否在 SP(Ti) 中。如果在，说明创建这个数据版本的事务在 Ti 开始时还处于活跃状态，那么 Ti 不应该看到这个数据版本。这是为了保证事务的隔离性，避免事务 Ti 看到其他未提交事务的中间状态，从而防止脏读等问题。
     * 举个例子，假设事务 T1 开始时，记录了活跃事务集合 SP(T1) = {T2, T3}。之后，事务 T2 创建了一个数据版本，其 XMIN 就是 T2 的事务 ID。当 T1 尝试访问这个数据版本时，由于 XMIN（T2 的事务 ID）在 SP(T1) 中，所以这个数据版本对 T1 是不可见的。这就保证了 T1 只能看到已经提交的事务对数据的修改，从而保证了事务的隔离性。
     **/

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true;

        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if(xmax == 0) return true;
            if(xmax != xid) {
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

}
