package top.guoziyang.mydb.backend.dm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.common.SubArray;
import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.dm.logger.Logger;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.page.PageX;
import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;

public class Recover {

    private static final byte LOG_TYPE_INSERT = 0;//插入类型
    private static final byte LOG_TYPE_UPDATE = 1;//更新类型

    private static final int REDO = 0;
    private static final int UNDO = 1;


    static class InsertLogInfo {
        //事务ID，用于标识事务的唯一性
        long xid;
        // 页面号，指示插入操作发生在哪个数据库页面上
        int pgno;
        // 偏移量，指定在页面内的插入位置
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        // 事务ID，用于标识事务的唯一性
        long xid;
        // 页面号
        int pgno;
        // 偏移量
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    /**
     * 这是一个高层次的恢复方法，负责整个数据库恢复流程的调度和管理,最后恢复数据库到一致状态。
     * 该方法会执行一系列操作，包括截断页面缓存、重做已提交事务的日志以及回滚未提交事务的日志。
     *
     * @param tm 事务管理器，用于管理事务的状态
     * @param lg 日志记录器，用于读取和处理日志
     * @param pc 页面缓存，用于管理数据库页面
     */
    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        // 输出恢复开始的信息
        System.out.println("Recovering...");

        // 将日志记录器的指针重置到日志文件的开头
        lg.rewind();
        // 初始化最大页面号为 0
        int maxPgno = 0;
        // 循环读取日志记录，直到没有更多日志
        while(true) {
            // 从日志记录器中读取下一条日志 next方法返回的是日志的data部分
            byte[] log = lg.next();
            // 如果没有更多日志，跳出循环
            if(log == null) break;
            // 定义变量用于存储当前日志涉及的页面号
            int pgno;
            // 判断当前日志是否为插入类型的日志
            if(isInsertLog(log)) {
                // 如果是插入类型的日志，解析日志信息
                InsertLogInfo li = parseInsertLog(log);
                // 获取插入日志涉及的页面号
                pgno = li.pgno;
            } else {
                // 如果是更新类型的日志，解析日志信息
                UpdateLogInfo li = parseUpdateLog(log);
                // 获取更新日志涉及的页面号
                pgno = li.pgno;
            }
            // 如果当前日志涉及的页面号大于最大页面号
            if(pgno > maxPgno) {
                // 更新最大页面号为当前页面号
                maxPgno = pgno;
            }
        }
        // 如果最大页面号为 0，将其设置为 1
        if(maxPgno == 0) {
            maxPgno = 1;
        }
        // 根据最大页面号截断页面缓存
        pc.truncateByBgno(maxPgno);
        // 输出截断操作的信息
        System.out.println("Truncate to " + maxPgno + " pages.");

        // 执行重做操作，恢复已提交事务的日志
        redoTranscations(tm, lg, pc);
        // 输出重做操作完成的信息
        System.out.println("Redo Transactions Over.");

        // 执行回滚操作，撤销未提交事务的日志
        undoTranscations(tm, lg, pc);
        // 输出回滚操作完成的信息
        System.out.println("Undo Transactions Over.");

        // 输出恢复完成的信息
        System.out.println("Recovery Over.");
    }
    /**
     * 对已提交的事务日志进行重做操作。
     * 该方法会遍历日志文件，针对已提交的事务（即非活跃事务），执行相应的重做操作，
     * 确保数据库在故障恢复后，已提交事务的更改能够正确应用到数据库中。
     *
     * @param tm 事务管理器，用于判断事务是否处于活跃状态
     * @param lg 日志记录器，用于读取日志文件
     * @param pc 页面缓存，用于管理数据库页面
     */
    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        // 将日志记录器的指针重置到日志文件的开头，以便从头开始读取日志
        lg.rewind();
        // 循环读取日志记录，直到没有更多日志
        while(true) {
            // 从日志记录器中读取下一条日志
            byte[] log = lg.next();
            // 如果没有更多日志，跳出循环
            if(log == null) break;
            // 判断当前日志是否为插入类型的日志
            if(isInsertLog(log)) {
                // 如果是插入类型的日志，解析日志信息
                InsertLogInfo li = parseInsertLog(log);
                // 获取插入日志对应的事务ID
                long xid = li.xid;
                // 判断事务是否已经提交（即非活跃状态）
                if(!tm.isActive(xid)) {
                    // 如果事务已提交，执行插入日志的重做操作
                    doInsertLog(pc, log, REDO);
                }
            } else {
                // 如果是更新类型的日志，解析日志信息
                UpdateLogInfo xi = parseUpdateLog(log);
                // 获取更新日志对应的事务ID
                long xid = xi.xid;
                // 判断事务是否已经提交（即非活跃状态）
                if(!tm.isActive(xid)) {
                    // 如果事务已提交，执行更新日志的重做操作
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }
    /**
     * 对未提交的事务日志进行回滚操作。
     * 该方法会遍历日志文件，针对未提交的事务（即活跃事务），执行相应的回滚操作，
     * 确保数据库在故障恢复后，未提交事务的更改能够被撤销，恢复到事务开始前的状态。
     *
     * @param tm 事务管理器，用于判断事务是否处于活跃状态
     * @param lg 日志记录器，用于读取日志文件
     * @param pc 页面缓存，用于管理数据库页面
     */

    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有active log进行倒序undo
        for(Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size()-1; i >= 0; i --) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE+1;
    private static final int OF_UPDATE_UID = OF_XID+8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID+8;


    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }


    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return li;
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;
        if(flag == REDO) {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID+8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO+4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET+2;

    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }


    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pc.getPage(li.pgno);
        } catch(Exception e) {
            Panic.panic(e);
        }
        try {
            if(flag == UNDO) {
                DataItem.setDataItemRawInvalid(li.raw);
            }
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            pg.release();
        }
    }
}
