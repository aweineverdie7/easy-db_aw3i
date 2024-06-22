/*
 *@Type NormalStore.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:07
 * @version
 */
package service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import controller.SocketServerHandler;
import model.command.Command;
import model.command.CommandPos;
import model.command.RmCommand;
import model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandUtil;
import utils.LoggerUtil;
import utils.RandomAccessFileUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.jar.JarEntry;

public class NormalStore implements Store {

    public static final String TABLE = ".table";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";


    /**
     * 内存表，类似缓存
     */
    private TreeMap<String, Command> memTable;

    /**
     * hash索引，存的是数据长度和偏移量
     * */
    private HashMap<String, CommandPos> index;

    /**
     * 数据目录
     */
    private final String dataDir;

    /**
     * 读写锁，支持多线程，并发安全写入
     */
    private final ReadWriteLock indexLock;

    /**
     * 暂存数据的日志句柄
     */
    private RandomAccessFile writerReader;

    /**
     * 持久化阈值
     */
    private final int storeThreshold = 3;

    public NormalStore(String dataDir) {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.memTable = new TreeMap<String, Command>();
        this.index = new HashMap<>();

        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER,logFormat, "NormalStore","dataDir isn't exist,creating...");
            file.mkdirs();
        }
        this.reloadIndex();
    }

    /**
     * 生成文件路径.
     * 此方法通过组合数据目录路径、一个固定的名称和一个表标识符来生成一个唯一的文件路径。
     * 主要用于存储或检索与特定表相关联的数据文件。路径的构造依赖于系统的文件分隔符，
     * 确保了路径在不同操作系统上的兼容性。
     *
     * @return 返回构造的文件路径字符串。
     */
    public String genFilePath() {
        // 使用文件分隔符连接数据目录、名称和表标识符以构造文件路径
        return this.dataDir + File.separator + NAME + TABLE;
    }



    /**
     * 重新加载索引。从文件中读取命令数据，并根据命令内容更新索引。
     * 这个方法用于在程序运行时动态更新索引，以便快速定位和执行历史命令。
     */
    public void reloadIndex() {
        try {
            // 打开文件以进行随机访问读写。
            RandomAccessFile file = new RandomAccessFile(this.genFilePath(), RW_MODE);
            // 获取文件长度，用于后续循环读取文件。
            long len = file.length();
            long start = 0;
            file.seek(start);
            // 从文件开始位置循环读取，直到读取到文件末尾。
            while (start < len) {
                // 读取命令长度。
                int cmdLen = file.readInt();
                // 根据命令长度创建字节数组，用于存储读取的命令数据。
                byte[] bytes = new byte[cmdLen];
                // 读取命令数据。
                file.read(bytes);
                // 将字节数组转换为字符串，然后解析为JSONObject。
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                // 根据JSONObject解析出具体的命令对象。
                Command command = CommandUtil.jsonToCommand(value);
                start += 4;
                // 如果命令对象不为空，则构建命令位置对象，并更新索引。
                if (command != null) {
                    CommandPos cmdPos = new CommandPos((int) start, cmdLen);
                    index.put(command.getKey(), cmdPos);
                }
                // 更新当前读取位置到下一个命令的起始位置。
                start += cmdLen;
            }
            // 将文件读取指针移动到文件末尾，为后续写入做准备。
            file.seek(file.length());
        } catch (Exception e) {
            // 捕获并打印异常信息。
            e.printStackTrace();
        }
        // 日志记录索引加载情况。
        LoggerUtil.debug(LOGGER, logFormat, "reload index: "+index.toString());
    }


    /**
     * 将内存表中的命令刷新到磁盘。
     * 此方法确保在并发环境下的安全性，通过同步关键字synchronized限制同时只有一个线程可以执行刷新操作。
     * 内存表为空时，直接返回，不进行任何操作。
     * 写入磁盘的过程包括：为每个命令生成字节码、写入长度、写入实际的字节码内容，并在写入完成后更新索引。
     * 如果在写入过程中发生IOException，将抛出RuntimeException。
     */
    private synchronized void flushMemTableToDisk() {
        // 如果内存表为空，则无需进行刷新操作
        if (memTable.isEmpty()) return; // 如果没有数据需要刷盘，直接返回

        // 创建一个临时索引用于存储即将写入磁盘的命令的位置信息
        // 可能需要临时的索引映射来记录新的位置信息
        HashMap<String, CommandPos> tempIndex = new HashMap<>();

        // 遍历内存表中的每个命令
        // 遍历内存表，将每个Command写入到磁盘
        for (HashMap.Entry<String, Command> entry : memTable.entrySet()) {
            Command command = entry.getValue();
            // 将命令序列化为JSON字节码
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 写入命令字节码的长度，用于后续读取时定位命令的起始位置
            RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
            // 写入命令字节码到磁盘，并记录写入的位置信息
            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            // 将命令的位置信息添加到临时索引中
            tempIndex.put(entry.getKey(), cmdPos); // 使用临时索引记录
        }
        // 更新索引为临时索引，完成刷新操作
        // 用新的索引替换旧索引
        this.index = tempIndex;

        // 清空内存表，为新的命令预留空间
        memTable.clear();
    }

    /**
     * 设置键值对。
     *
     * @param key 键，用于唯一标识一个值。
     * @param value 值，与键相关联的具体内容。
     * @throws RuntimeException 如果在设置过程中发生任何异常。
     */
    @Override
    public void set(String key, String value) {
        try {
            // 创建SetCommand对象，用于封装设置操作的键值对信息。
            SetCommand command = new SetCommand(key, value);

            // 获取写锁，以确保并发操作时的线程安全。
            // 加锁
            indexLock.writeLock().lock();
            try {
                // 在文件中写入命令的长度，用于后续读取时定位命令位置。
                // TODO://先写内存表，内存表达到一定阀值再写进磁盘
                // 先更新内存表
                memTable.put(key, command);
                // 判断是否需要刷盘
                if (memTable.size() >= storeThreshold) {
                    flushMemTableToDisk();
                }
                // TODO://判断是否需要将内存表中的值写回table
            } finally {
                // 释放写锁。
            }
        } catch (Throwable t) {
            // 如果发生任何异常，抛出运行时异常。
            throw new RuntimeException(t);
        } finally {
            // 确保在方法退出时释放写锁。
            indexLock.writeLock().unlock();
        }
    }


    /**
     * 根据键获取值。
     * 此方法用于从存储中检索与给定键相关联的值。它首先通过键从索引中定位命令的位置，
     * 然后从该位置读取命令的字节数据。最后，它解析命令对象并根据命令类型返回相关的值。
     *
     * @param key 需要检索的键。
     * @return 与键相关联的值，如果键不存在或命令类型不支持，则返回null。
     * @throws RuntimeException 如果在读取或解析过程中发生任何异常。
     */
    @Override
    public String get(String key) {
        try {
            indexLock.readLock().lock();

            // 先检查内存缓存
            Command cachedCommand = memTable.get(key);
            if (cachedCommand != null) {
                if (cachedCommand instanceof SetCommand) {
                    return ((SetCommand) cachedCommand).getValue();
                } else if (cachedCommand instanceof RmCommand) {
                    return null;
                }
            }

            // 再检查磁盘数据
            CommandPos cmdPos = index.get(key);
            if (cmdPos == null) {
                return null;
            }
            byte[] commandBytes = RandomAccessFileUtil.readByIndex(this.genFilePath(), cmdPos.getPos(), cmdPos.getLen());
            JSONObject value = JSONObject.parseObject(new String(commandBytes));
            Command cmd = CommandUtil.jsonToCommand(value);
            if (cmd instanceof SetCommand) {
                return ((SetCommand) cmd).getValue();
            }
            if (cmd instanceof RmCommand) {
                return null;
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.readLock().unlock();
        }
        return null;
    }

    /**
     * 删除指定键的数据。
     * 使用JSON序列化命令对象，加锁以确保线程安全，将命令写入到日志文件（table）中，并更新索引。
     * 在出现异常时，抛出运行时异常，并在最终确保释放写锁。
     *
     * @param key 要删除的数据的键。
     */
    @Override
    public void rm(String key) {
        try {
            // 创建删除命令对象。
            RmCommand command = new RmCommand(key);
            // 将命令对象序列化为JSON字节。
            byte[] commandBytes = JSONObject.toJSONBytes(command);

            // 获取写锁以确保线程安全。
            // 加锁
            indexLock.writeLock().lock();
            // TODO://先写内存表，内存表达到一定阀值再写进磁盘

            // 先更新内存表
            memTable.put(key, command);

            // 判断是否需要刷盘
            if (memTable.size() >= storeThreshold) {
                flushMemTableToDisk();
            }

            // TODO://判断是否需要将内存表中的值写回table

        } catch (Throwable t) {
            // 如果出现任何异常，抛出运行时异常。
            throw new RuntimeException(t);
        } finally {
            // 确保在方法结束时释放写锁。
            indexLock.writeLock().unlock();
        }
    }


    @Override
    public void close() throws IOException {

    }
}
