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
import java.nio.ByteBuffer;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static utils.CompressionUtils.decompressGZFile;

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

/**
 * 初始化文件计数器。
 * 该方法用于确定当前数据目录中最大文件编号，并返回其加一值，用于生成新文件的名称。
 * 文件名应以特定格式开始，后跟一个数字和表名，可能以.gz结尾。
 *
 * @return 返回最大文件编号加一后的值，如果目录中没有文件，则返回0。
 */
private int initializeFileCounter() {
    // 创建数据目录的File对象
    File dir = new File(dataDir);
    // 使用文件过滤器列出所有以指定名称开始，以表名或.gz表名结尾的文件
    File[] files = dir.listFiles((d, name) -> name.startsWith(NAME) && (name.endsWith(TABLE) || name.endsWith(TABLE)));

    // 如果目录为空或没有符合条件的文件，返回0
    if (files == null || files.length == 0) {
        return 0;
    }

    int maxCounter = 0;
    // 编译正则表达式，用于匹配文件名中的数字部分
    Pattern pattern = Pattern.compile(NAME + "(\\d+)" + TABLE );
    // 遍历所有文件，寻找最大文件编号
    for (File file : files) {
        // 使用正则表达式匹配文件名
        Matcher matcher = pattern.matcher(file.getName());
        // 如果匹配成功
        if (matcher.find()) {
            // 解析文件名中的数字部分
            int counter = Integer.parseInt(matcher.group(1));
            // 更新最大文件编号
            if (counter > maxCounter) {
                maxCounter = counter;
            }
        }
    }
    // 返回最大文件编号加一后的值
    return maxCounter + 1;
}




    public String genFilePath(int fileCounter) {
        return this.dataDir + File.separator + NAME + fileCounter + TABLE;
    }

    public String getCurrentFilePath() {
        return this.dataDir + File.separator + NAME + TABLE;
    }
    public String getGzFilePath(int fileCounter){
        return this.dataDir + File.separator + NAME + fileCounter + TABLE + ".gz";
    }


public void reloadIndex() {
    try {
        for (int i = 0; i < fileCounter; i++) { // 遍历所有文件
            String filePath = this.genFilePath(i);
            long fileLength = new File(filePath).length();
            if(filePath.endsWith(TABLE)) {
                // 使用获取到的文件长度作为读取长度，从位置0开始读取
                RandomAccessFile currentFile = new RandomAccessFile(filePath, RW_MODE);
                loadCommandsFromStream(new FileInputStream(currentFile.getFD()));
            }
//            else {
//                // 处理非压缩文件
//                RandomAccessFile file = new RandomAccessFile(filePath, RW_MODE);
//                loadCommandsFromStream(new FileInputStream(file.getFD()));
//            }
        }
        // 处理当前的 data.table 文件
            RandomAccessFile currentFile = new RandomAccessFile(this.getCurrentFilePath(), RW_MODE);
            loadCommandsFromStream(new FileInputStream(currentFile.getFD()));

    } catch (Exception e) {
        e.printStackTrace();
    }
    LoggerUtil.debug(LOGGER, logFormat, "reload index: " + index.toString());
}

/**
 * 从输入流中加载命令。
 * 此方法用于解析输入流中的命令数据，每条命令由长度前缀和JSON格式的数据组成。
 * 它将解析出的命令存储到索引中，以便后续可以快速访问和执行这些命令。
 *
 * @param inputStream 输入流，包含待加载的命令数据。
 * @throws IOException 如果在读取输入流时发生错误。
 */
private void loadCommandsFromStream(InputStream inputStream) throws IOException {
    // 使用BufferedInputStream提高读取效率
    try (BufferedInputStream bis = new BufferedInputStream(inputStream)) {
        // 用于存储长度前缀的字节数组
        byte[] lengthBytes = new byte[4];

        // 循环读取输入流中的数据，直到读取结束
        while (bis.read(lengthBytes) != -1) {
            // 将长度前缀字节数组转换为整数，表示当前命令的长度
            int cmdLen = ByteBuffer.wrap(lengthBytes).getInt();
            // 创建一个字节数组，用于存储读取的命令数据
            byte[] bytes = new byte[cmdLen];
            // 从输入流中读取命令数据
            bis.read(bytes);
            // 将字节数组转换为字符串，然后解析为JSONObject
            JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
            // 将JSON对象转换为Command对象
            Command command = CommandUtil.jsonToCommand(value);
            // 如果转换成功，则将命令及其长度存储到索引中
            if (command != null) {
                CommandPos cmdPos = new CommandPos(-1, cmdLen);
                index.put(command.getKey(), cmdPos);
            }
        }
    }
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
        this.writerReader = new RandomAccessFile(this.getCurrentFilePath(), RW_MODE);
        // 检查当前文件的大小是否达到轮转的阈值
        if (this.writerReader.length() >= FILE_SIZE_THRESHOLD) {
            // 如果达到阈值，则执行文件轮转操作
            rotateFile();
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
        //TODO:异步压缩文件，将table文件去重
        compressFile(rotatedFilePath);
        rotateLock.unlock();
    }

    /**
     * 压缩文件，保留相同key的最后命令。
     *
     * @param filePath 要压缩的文件路径。
     * @throws IOException 如果在压缩过程中发生I/O错误。
     */
    private void compressFile(String filePath) throws IOException {
        // 创建一个临时文件用于写入压缩后的数据
        String tempFilePath = filePath + ".tmp";
        RandomAccessFile tempFile = new RandomAccessFile(tempFilePath, RW_MODE);

        // 用于存储最后命令的Map
        HashMap<String, Command> lastCommands = new HashMap<>();

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


        // 遍历内存表中的每个命令
        // 遍历内存表，将每个Command写入到磁盘
        for (HashMap.Entry<String, Command> entry : memTable.entrySet()) {
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
            //对当前活跃的data.table文件的直接访问逻辑
            CommandPos cmdPos = index.get(key);
            if (cmdPos != null) {
                byte[] commandBytes = RandomAccessFileUtil.readByIndex(getCurrentFilePath(), cmdPos.getPos(), cmdPos.getLen());
                JSONObject value = JSONObject.parseObject(new String(commandBytes));
                Command cmd = CommandUtil.jsonToCommand(value);
                if (cmd instanceof SetCommand) {
                    return ((SetCommand) cmd).getValue();
                } else if (cmd instanceof RmCommand) {
                    return null;
                }
            }


            for (int i = fileCounter - 1; i >= 0; i--) { // 遍历所有文件
                if (cmdPos == null) continue;
                String filePath = this.genFilePath(i);
                byte[] commandBytes = RandomAccessFileUtil.readByIndex(filePath, cmdPos.getPos(), cmdPos.getLen());
                JSONObject value = JSONObject.parseObject(new String(commandBytes));
                Command cmd = CommandUtil.jsonToCommand(value);
                if (cmd instanceof SetCommand) {
                    return ((SetCommand) cmd).getValue();
                } else if (cmd instanceof RmCommand) {
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

//    @Override
//    public void exit() throws IOException {
//        this.close();
//    }
}



