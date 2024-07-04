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
import utils.LoggerUtil;
import utils.RandomAccessFileUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.ByteBuffer;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class NormalStore implements Store {

    public static final String TABLE = ".table";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";
    // 在类的成员变量中初始化一个线程池，可以根据需要调整线程池的配置
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);
    private final ReentrantLock rotateLock = new ReentrantLock();

    /**
     * 内存表，类似缓存
     */
    private TreeMap<String, Command> memTable;

    /**
     * 不可变内存表，用于持久化内存表中时暂存数据
     */
    private TreeMap<String, Command> immutable;

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
    private static final long FILE_SIZE_THRESHOLD = 1024 * 1024 * 1; // 1MB
    private final Lock mergeLock = new ReentrantLock();
    private final int MERGE_THRESHOLD = 5; // 假设当rotate次数达到5次时，进行

    public NormalStore(String dataDir) {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.memTable = new TreeMap<String, Command>();
        this.immutable = new TreeMap<String, Command>();
        this.index = new HashMap<>();

        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER,logFormat, "NormalStore","dataDir isn't exist,creating...");
            file.mkdirs();
        }
        this.reloadIndex();
        // 启动后台线程执行多文件压缩
        Thread mergeThread = new Thread(() -> {
            try {
                while (true) {
                    mergeAndCompressFiles();
                    Thread.sleep(1); // 每隔60秒执行一次合并和压缩操作
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        mergeThread.setDaemon(true); // 设置为后台线程
        mergeThread.start();
    }

/**
 * 初始化文件计数器。
 * 该方法用于确定当前数据目录中最大文件编号，并返回其加一值，用于生成新文件的名称。
 * 文件名应以特定格式开始，后跟一个数字和表名。
 *
 * @return 返回最大文件编号加一后的值，如果目录中没有文件，则返回0。
 */



private static final AtomicInteger fileCounter = new AtomicInteger(0);

public String genFilePath() {
    long timestamp = System.currentTimeMillis();
    String baseName = this.dataDir + File.separator + NAME + timestamp + TABLE;
    String fileName = baseName;
    int counter = fileCounter.getAndIncrement(); // 获取并增加计数器

    // 如果文件已存在，则循环尝试添加递增序号直到找到一个可用的文件名
    while (Files.exists(Paths.get(fileName))) {
        fileName = baseName + "_" + counter; // 添加序号作为后缀
        counter = fileCounter.getAndIncrement(); // 递增计数器
    }

    return fileName;
}


    public String getCurrentFilePath() {
        return this.dataDir + File.separator + NAME + TABLE;
    }


public void reloadIndex() {
    rotateLock.lock();
    try {
        File dataDirFile = new File(dataDir);
        File[] tableFiles = dataDirFile.listFiles((dir, name) -> name.startsWith(NAME) && name.endsWith(TABLE));

        if (tableFiles == null) {
            return;
        }
        for (File pfile : tableFiles) {
            String filePath = pfile.getAbsolutePath();
            if(filePath.endsWith(TABLE)) {
                RandomAccessFile file = new RandomAccessFile(filePath, RW_MODE);
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
        }
    } catch (Exception e) {
        e.printStackTrace();
    }
    finally {
        rotateLock.unlock();
    }
//    LoggerUtil.debug(LOGGER, logFormat, "reload index: " + index.toString());
}

    /**
     * 检查当前文件的大小，如果达到指定的阈值，则进行文件轮转。
     * 文件轮转通常是为了避免单个文件过大，导致处理效率下降或管理困难。
     * 此方法通过同步关键字确保在多线程环境下的安全性，避免多个线程同时尝试轮转文件导致的竞争条件。
     *
     * @throws IOException 如果在检查文件大小或执行文件轮转过程中发生I/O错误。
     */
    // 检查并执行rotate操作
    private void checkAndRotateIfNeeded() throws IOException {
        this.writerReader = new RandomAccessFile(this.getCurrentFilePath(), RW_MODE);
        // 检查当前文件的大小是否达到轮转的阈值
        if (this.writerReader.length() >= FILE_SIZE_THRESHOLD) {
            rotateFile();
            // 如果达到阈值，则执行文件轮转操作
        }
        this.writerReader.close();

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
        rotateLock.lock();
        try {
            // 执行那些真正需要独占访问的操作，比如关闭旧文件、打开新文件等
//            LOGGER.info("Rotating file due to size threshold.");
            //关闭流me
            if (this.writerReader != null) {
                this.writerReader.close();
            }
            // 生成滚动后的文件路径。
            String rotatedFilePath = genFilePath();
            // 将当前的日志文件移动到滚动后的路径，实质上是进行了重命名。
            Files.move(Paths.get(getCurrentFilePath()), Paths.get(rotatedFilePath));
            // 创建一个新的RandomAccessFile实例，用于写入新的日志文件。
            this.writerReader = new RandomAccessFile(getCurrentFilePath(), RW_MODE);
            //TODO:异步压缩文件，将table文件去重
            executorService.submit(() -> {
                try {
//                mergeAndCompressFiles();
                    compressFile(rotatedFilePath);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
//            LOGGER.info("File rotation completed.");
        } catch (IOException e) {
//            LOGGER.error("File rotation failed.", e);
            // 异常处理
        } finally {
            rotateLock.unlock();
        }

    }

    /**
     * 压缩文件，保留相同key的最后命令。
     *
     * @param filePath 要压缩的文件路径。
     * @throws IOException 如果在压缩过程中发生I/O错误。
     */
    private void compressFile(String filePath) throws IOException {
        try {
            // 创建一个临时文件用于写入压缩后的数据
            String tempFilePath = filePath + ".tmp";
            RandomAccessFile tempFile = new RandomAccessFile(tempFilePath, RW_MODE);

            // 用于存储最后命令的Map
            TreeMap<String, Command> lastCommands = new TreeMap<>();

            // 读取原始文件并填充lastCommands Map
            try (RandomAccessFile originalFile = new RandomAccessFile(filePath, RW_MODE)) {
                byte[] lengthBytes = new byte[4];

                while (originalFile.read(lengthBytes) != -1) {
                    int cmdLen = ByteBuffer.wrap(lengthBytes).getInt();
                    byte[] commandBytes = new byte[cmdLen];
                    originalFile.read(commandBytes);

                    JSONObject value = JSON.parseObject(new String(commandBytes, StandardCharsets.UTF_8));
                    Command command = CommandUtil.jsonToCommand(value);

                    if (command != null) {
                        lastCommands.put(command.getKey(), command);
                    }
                }
            }

            // 将最后命令写入临时文件
            for (Command command : lastCommands.values()) {
                byte[] commandBytes = JSONObject.toJSONBytes(command);
                tempFile.writeInt(commandBytes.length);
                tempFile.write(commandBytes);
            }

            // 关闭临时文件
            tempFile.close();

            // 替换原始文件为压缩后的文件
            Files.move(Paths.get(tempFilePath), Paths.get(filePath), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
        }
    }


private void mergeAndCompressFiles() throws IOException {
    rotateLock.lock();
    try {
        File dataDirFile = new File(dataDir);
        File[] tableFiles = dataDirFile.listFiles((dir, name) -> name.startsWith(NAME) && name.endsWith(TABLE));

        if (tableFiles == null) {
            return;
        }
        List<File> numberedTableFiles = new ArrayList<>();
        Pattern pattern = Pattern.compile("^" + NAME + "\\d+\\.table$");

        // 筛选出带有明确序号的文件
        for (File file : tableFiles) {
            if (pattern.matcher(file.getName()).matches()) {
                numberedTableFiles.add(file);
            }
        }

        if (numberedTableFiles.isEmpty() || numberedTableFiles.size() <= MERGE_THRESHOLD) {
            return; // 如果符合条件的文件数量小于等于合并阈值，则无需合并
        }

        // 对文件按修改时间升序排序，最早的文件优先合并
        Collections.sort(numberedTableFiles, Comparator.comparingLong(File::lastModified));


        // 使用TreeMap来辅助去重和保持最新命令，键为命令的键，值为命令对象
        TreeMap<String, Command> mergedCommands = new TreeMap<>();

        // 遍历需要合并的文件
        for (File file : numberedTableFiles) {
            if (mergedCommands.size() >= MERGE_THRESHOLD) break; // 达到合并文件数上限，停止读取更多文件

            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                byte[] lengthBytes = new byte[4];

                while (raf.read(lengthBytes) != -1) {
                    int cmdLen = ByteBuffer.wrap(lengthBytes).getInt();
                    byte[] commandBytes = new byte[cmdLen];
                    raf.read(commandBytes);

                    JSONObject value = JSON.parseObject(new String(commandBytes, StandardCharsets.UTF_8));
                    Command command = CommandUtil.jsonToCommand(value);

                    if (command != null) {
                        // 使用TreeMap的put方法自动去重，仅保留键对应的最新命令
                        mergedCommands.put(command.getKey(), command);
                    }
                }
            }
        }

        // 将合并后的命令写入到一个新的文件中
        String mergedFilePath = genFilePath();
        try (RandomAccessFile mergedFile = new RandomAccessFile(mergedFilePath, RW_MODE)) {
            for (Command command : mergedCommands.values()) {
                byte[] commandBytes = JSONObject.toJSONBytes(command);
                mergedFile.writeInt(commandBytes.length);
                mergedFile.write(commandBytes);
            }
        }

        // 删除已合并的旧文件
        for (File file : numberedTableFiles) {
            Files.deleteIfExists(file.toPath());
        }
    } catch (IOException e) {
        throw new RuntimeException(e);
    } finally {
        rotateLock.unlock();
    }
}


    /**
     * 将内存表中的命令刷新到磁盘。
     * 此方法确保在并发环境下的安全性，通过同步关键字synchronized限制同时只有一个线程可以执行刷新操作。
     * 内存表为空时，直接返回，不进行任何操作。
     * 写入磁盘的过程包括：为每个命令生成字节码、写入长度、写入实际的字节码内容，并在写入完成后更新索引。
     * 如果在写入过程中发生IOException，将抛出RuntimeException。
     */
    private void flushMemTableToDisk() throws IOException {

        // 如果内存表为空，则无需进行刷新操作
        if (immutable.isEmpty()) return; // 如果没有数据需要刷盘，直接返回


        // 遍历内存表中的每个命令
        // 遍历内存表，将每个Command写入到磁盘
        for (HashMap.Entry<String, Command> entry : immutable.entrySet()) {
            Command command = entry.getValue();
            // 将命令序列化为JSON字节码
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 写入命令字节码的长度，用于后续读取时定位命令的起始位置
            RandomAccessFileUtil.writeInt(this.getCurrentFilePath(), commandBytes.length);
            // 写入命令字节码到磁盘，并记录写入的位置信息
            long pos = RandomAccessFileUtil.write(this.getCurrentFilePath(), commandBytes);
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            // 将命令的位置信息添加到索引中
            this.index.put(entry.getKey(), cmdPos);
        }


        // 清空内存表，为新的命令预留空间
        immutable.clear();
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
                    switchMemTable();
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
                if (cachedCommand == null) {
                    cachedCommand = immutable.get(key);
                }
                if (cachedCommand != null) {
                    if (cachedCommand instanceof SetCommand) {
                        return ((SetCommand) cachedCommand).getValue();
                    } else if (cachedCommand instanceof RmCommand) {
                        return null;
                    }
                }
                this.reloadIndex();
                //对当前活跃的data.table文件的直接访问逻辑
                CommandPos cmdPos = index.get(key);
                try {
                    File dataDirFile = new File(dataDir);
                    File[] tableFiles = dataDirFile.listFiles((dir, name) -> name.startsWith(NAME) && name.endsWith(TABLE));

                    if (tableFiles == null) {
                        return null;
                    }

                    // 筛选出带有明确序号的文件
                    for (File file : tableFiles) {
                        String filePath = file.getAbsolutePath();
                        if(filePath.endsWith(TABLE)) {
                            // 使用获取到的文件长度作为读取长度，从位置0开始读取
                            if (cmdPos == null) continue;
                            byte[] commandBytes = RandomAccessFileUtil.readByIndex(filePath, cmdPos.getPos(), cmdPos.getLen());
                            JSONObject value = JSONObject.parseObject(new String(commandBytes));
                            Command cmd = CommandUtil.jsonToCommand(value);
                            if (cmd instanceof SetCommand) {
                                return ((SetCommand) cmd).getValue();
                            } else if (cmd instanceof RmCommand) {
                                return null;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
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
                switchMemTable();
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


    private void switchMemTable() {
        immutable = memTable;
        memTable = new TreeMap<>();
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



