package top.guoziyang.mydb.backend.dm.logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

/**
 * 日志文件读写
 * 
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 * BadTail 是在数据库崩溃时，没有来得及写完的日志数据,不一定存在
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int
 */
public class LoggerImpl implements Logger {

    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;
    
    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

        private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;//所有日志的校验和

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();
    }

    // 检查并移除bad tail
    private void checkAndRemoveTail() {
        rewind();

        int xCheck = 0;
        while(true) {
            byte[] log = internNext();
            if(log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        if(xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }

        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }
    // 计算日志的checksum
    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXChecksum(log);
    }

    private void updateXChecksum(byte[] log) {
        // 计算新的全局校验和
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            // 将文件通道的位置设置为文件开头
            fc.position(0);
            // 将更新后的全局校验和转换为字节数组，并包装成 ByteBuffer
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            // 强制将通道中的数据刷写到磁盘
            fc.force(false);
        } catch(IOException e) {
            // 若出现 IO 异常，调用 Panic.panic 方法处理
            Panic.panic(e);
        }
    }

    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }
    /**
     * 内部方法，用于读取下一条日志记录。
     * 该方法会检查文件剩余长度是否足够读取一条完整的日志记录，
     * 并验证日志记录的校验和是否正确。
     *
     * @return 如果存在下一条有效的日志记录，则返回该日志记录的字节数组；否则返回 null。
     */
    private byte[] internNext() {
        // 检查当前位置加上日志头部长度是否超过文件大小，如果超过则表示没有更多日志，返回 null
        if(position + OF_DATA >= fileSize) {
            return null;
        }
        // 分配一个 4 字节的 ByteBuffer 用于读取日志记录的大小
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            // 将文件通道的位置设置为当前位置
            fc.position(position);
            // 从文件通道中读取 4 字节到 tmp 中
            fc.read(tmp);
        } catch(IOException e) {
            // 若读取过程中出现异常，调用 Panic.panic 方法处理
            Panic.panic(e);
        }
        // 将 tmp 中的字节转换为整数，得到日志记录的数据部分的字节长度
        int size = Parser.parseInt(tmp.array());
        // 检查当前位置加上日志记录总长度是否超过文件大小，如果超过则表示没有完整的日志，返回 null
        if(position + size + OF_DATA > fileSize) {
            return null;
        }

        // 分配一个足够大的 ByteBuffer 用于读取完整的日志记录（包括头部和数据）
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            // 将文件通道的位置设置为当前位置
            fc.position(position);
            // 从文件通道中读取完整的日志记录到 buf 中
            fc.read(buf);
        } catch(IOException e) {
            // 若读取过程中出现异常，调用 Panic.panic 方法处理
            Panic.panic(e);
        }

        // 将 buf 中的数据转换为字节数组
        byte[] log = buf.array();
        // 计算日志记录数据部分的校验和
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        // 从日志记录中提取存储的校验和
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        // 比较计算得到的校验和与存储的校验和是否一致，如果不一致则返回 null
        if(checkSum1 != checkSum2) {
            return null;
        }
        // 更新当前位置，指向下一条日志记录的起始位置
        position += log.length;
        // 返回完整的日志记录字节数组
        return log;
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }
    
}
