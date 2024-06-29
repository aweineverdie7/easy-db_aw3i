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
import model.command.Command;
import model.command.CommandPos;
import model.command.RmCommand;
import model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandUtil;
import utils.CompressionUtils;
import utils.LoggerUtil;
import utils.RandomAccessFileUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    private final int storeThreshold = 1000;
    private static final long FILE_SIZE_THRESHOLD = 1024 * 1024 * 10; // 10MB

    private int rotateIndex = 0; // 用于rotate文件的序号
    private final Lock rotateLock; // 新增的旋转锁
    private int fileCounter;
    public NormalStore(String dataDir) {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.rotateLock = new ReentrantLock(); // 初始化旋转锁
        this.memTable = new TreeMap<String, Command>();
        this.index = new HashMap<>();

        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER,logFormat, "NormalStore","dataDir isn't exist,creating...");
            file.mkdirs();
        }
        this.fileCounter = initializeFileCounter();
        this.reloadIndex();
    }

    private int initializeFileCounter() {
        File dir = new File(dataDir);
        File[] files = dir.listFiles((d, name) -> name.startsWith(NAME) && name.endsWith(TABLE));
        if (files == null || files.length == 0) {
            return 0;
        }

        int maxCounter = 0;
        for (File file : files) {
            String fileName = file.getName();
            if (fileName.equals(NAME + TABLE)) {
                continue; // 跳过当前的 data.table 文件
            }
            String counterStr = fileName.substring(NAME.length(), fileName.length() - TABLE.length());
            int counter = Integer.parseInt(counterStr);
            if (counter > maxCounter) {
                maxCounter = counter;
            }
        }
        return maxCounter + 1;
    }

    public String genFilePath(int fileCounter) {
        return this.dataDir + File.separator + NAME + fileCounter + TABLE;
    }

    public String getCurrentFilePath() {
        return this.dataDir + File.separator + NAME + TABLE;
    }


    /**
     * 重新加载索引。从文件中读取命令数据，并根据命令内容更新索引。
     * 这个方法用于在程序运行时动态更新索引，以便快速定位和执行历史命令。
     */
    public void reloadIndex() {
        try {
            for (int i = 0; i < fileCounter; i++) { // 遍历所有文件
                RandomAccessFile file = new RandomAccessFile(this.genFilePath(i), RW_MODE);
                long len = file.length();
                long start = 0;
                file.seek(start);
                while (start < len) {
                    int cmdLen = file.readInt();
                    byte[] bytes = new byte[cmdLen];
                    file.read(bytes);
                    JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                    Command command = CommandUtil.jsonToCommand(value);
                    start += 4;
                    if (command != null) {
                        CommandPos cmdPos = new CommandPos((int) start, cmdLen);
                        index.put(command.getKey(), cmdPos);
                    }
                    start += cmdLen;
                }
                file.seek(file.length());
            }
            // 处理当前的 data.table 文件
            RandomAccessFile currentFile = new RandomAccessFile(this.getCurrentFilePath(), RW_MODE);
            this.writerReader = currentFile;
            long len = currentFile.length();
            long start = 0;
            currentFile.seek(start);
            while (start < len) {
                int cmdLen = currentFile.readInt();
                byte[] bytes = new byte[cmdLen];
                currentFile.read(bytes);
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                Command command = CommandUtil.jsonToCommand(value);
                start += 4;
                if (command != null) {
                    CommandPos cmdPos = new CommandPos((int) start, cmdLen);
                    index.put(command.getKey(), cmdPos);
                }
                start += cmdLen;
            }
            currentFile.seek(currentFile.length());
        } catch (Exception e) {
            e.printStackTrace();
        }
        LoggerUtil.debug(LOGGER, logFormat, "reload index: " + index.toString());
    }


    /**
     * 检查当前文件的大小，如果达到指定的阈值，则进行文件轮转。
     * 文件轮转通常是为了避免单个文件过大，导致处理效率下降或管理困难。
     * 此方法通过同步关键字确保在多线程环境下的安全性，避免多个线程同时尝试轮转文件导致的竞争条件。
     *
     * @throws IOException 如果在检查文件大小或执行文件轮转过程中发生I/O错误。
     */
    // 检查并执行rotate操作
    private synchronized void checkAndRotateIfNeeded() throws IOException {
        // 检查当前文件的大小是否达到轮转的阈值
        if (this.writerReader.length() >= FILE_SIZE_THRESHOLD) {
            // 如果达到阈值，则执行文件轮转操作
            rotateFile();
        }
    }
    /**
     * 执行日志文件的滚动操作。
     * 当需要滚动日志文件时，此方法将当前正在写入的日志文件重命名并压缩，然后创建一个新的日志文件以继续写入。
     * 这个方法使用了一个互斥锁来确保在滚动操作期间不会有其他线程尝试写入日志文件，从而保证了操作的原子性。
     *
     * @throws IOException 如果在移动文件或创建新文件时发生I/O错误。
     */
    // 执行rotate操作
    private void rotateFile() throws IOException {
            //关闭流
        if (this.writerReader != null) {
            this.writerReader.close();
        }
        //开锁
        rotateLock.lock();
        // 生成滚动后的文件路径。
        String rotatedFilePath = genFilePath(fileCounter);
        // 将当前的日志文件移动到滚动后的路径，实质上是进行了重命名。
        Files.move(Paths.get(getCurrentFilePath()), Paths.get(rotatedFilePath));
        // 创建一个新的RandomAccessFile实例，用于写入新的日志文件。
        this.writerReader = new RandomAccessFile(getCurrentFilePath(), RW_MODE);
        fileCounter++; // 增加文件计数器
        // 异步压缩滚动后的日志文件，以减少滚动操作对当前写入操作的影响。
        CompressionUtils.compressFileAsync(rotatedFilePath,false);
        rotateLock.unlock();
    }




    /**
     * 将内存表中的命令刷新到磁盘。
     * 此方法确保在并发环境下的安全性，通过同步关键字synchronized限制同时只有一个线程可以执行刷新操作。
     * 内存表为空时，直接返回，不进行任何操作。
     * 写入磁盘的过程包括：为每个命令生成字节码、写入长度、写入实际的字节码内容，并在写入完成后更新索引。
     * 如果在写入过程中发生IOException，将抛出RuntimeException。
     */
    private synchronized void flushMemTableToDisk() throws IOException {

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
            RandomAccessFileUtil.writeInt(this.getCurrentFilePath(), commandBytes.length);
            // 写入命令字节码到磁盘，并记录写入的位置信息
            int pos = RandomAccessFileUtil.write(this.getCurrentFilePath(), commandBytes);
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            // 将命令的位置信息添加到临时索引中
            tempIndex.put(entry.getKey(), cmdPos); // 使用临时索引记录
        }
        // 更新索引为临时索引，完成刷新操作
        // 用新的索引替换旧索引
        this.index = tempIndex;

        // 清空内存表，为新的命令预留空间
        memTable.clear();
        // 检查是否需要rotate
        checkAndRotateIfNeeded();
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
                // 在文件中写入命令的长度，用于后续读取时定位命令位置。
                // TODO://先写内存表，内存表达到一定阀值再写进磁盘
                // 先更新内存表
                memTable.put(key, command);
                // 判断是否需要刷盘
                if (memTable.size() >= storeThreshold) {
                    flushMemTableToDisk();
                }
                // TODO://判断是否需要将内存表中的值写回table
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

            Command cachedCommand = memTable.get(key);
            if (cachedCommand != null) {
                if (cachedCommand instanceof SetCommand) {
                    return ((SetCommand) cachedCommand).getValue();
                } else if (cachedCommand instanceof RmCommand) {
                    return null;
                }
            }

            for (int i = fileCounter - 1; i >= 0; i--) { // 遍历所有文件
                CommandPos cmdPos = index.get(key);
                if (cmdPos == null) continue;
                byte[] commandBytes = RandomAccessFileUtil.readByIndex(this.genFilePath(i), cmdPos.getPos(), cmdPos.getLen());
                JSONObject value = JSONObject.parseObject(new String(commandBytes));
                Command cmd = CommandUtil.jsonToCommand(value);
                if (cmd instanceof SetCommand) {
                    return ((SetCommand) cmd).getValue();
                }
                if (cmd instanceof RmCommand) {
                    return null;
                }
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


/**
 * 关闭当前实例，并释放相关资源。
 * 此方法确保了在关闭过程中，所有的压缩任务得以完成，并且内存中的数据被刷新到磁盘。
 * 如果压缩执行器在指定时间内未能关闭，将强制关闭所有任务。
 *
 * @throws IOException 如果关闭过程中发生I/O错误。
 */
@Override
public void close() throws IOException {
    // 如果writerReader不为空，则尝试关闭它。
    if (writerReader != null) {
        writerReader.close();
    }
    flushMemTableToDisk();
    }
}
