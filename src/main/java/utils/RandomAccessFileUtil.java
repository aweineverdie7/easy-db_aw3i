/*
 *@Type RandomAccessFileUtil.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:58
 * @version
 */
package utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

public class RandomAccessFileUtil {

    private static final String RW_MODE = "rw";

    /**
     * 将字节数据写入到指定文件的末尾。
     *
     * @param filePath 文件路径，指定要写入的文件。
     * @param value 要写入文件的字节数据。
     * @return 返回写入文件后文件的总长度。
     * @throws IOException 如果发生文件操作相关的异常。
     */
    public static int write(String filePath, byte[] value) {
        // 使用RandomAccessFile来实现文件的读写操作。
        RandomAccessFile file = null;
        // 初始化文件长度为-1，用于后续返回写入后的文件长度。
        long len = -1L;
        try {
            // 以读写模式打开文件。
            file = new RandomAccessFile(filePath, RW_MODE);
            // 获取文件当前的长度，即后续写入数据的起始位置。
            len = file.length();
            // 将文件指针移动到文件末尾，准备写入新数据。
            file.seek(len);
            // 写入字节数据到文件。
            file.write(value);
            // 关闭文件流。
            file.close();
        } catch (Exception e) {
            // 打印异常堆栈跟踪信息。
            e.printStackTrace();
        }
        // 将文件长度转换为整型并返回，注意这里可能会发生数据类型转换溢出。
        return (int)len;
    }

    /**
     * 将一个整数写入到指定路径的文件末尾。
     *
     * @param filePath 文件的路径。指定的文件将被创建或打开以进行追加写入。
     * @param value 要写入文件的整数值。
     *
     * 注意：此方法捕获并打印任何异常，但不重新抛出。调用者需要确保对此方法的调用不会因异常而导致程序中断。
     */
    public static void writeInt(String filePath, int value) {
        // 使用RandomAccessFile来实现文件的读写操作，初始化为null。
        RandomAccessFile file = null;
        // 初始化文件长度变量，用于定位写入位置。
        long len = -1L;
        try {
            // 创建或打开文件，以追加模式进行写操作。
            file = new RandomAccessFile(filePath, RW_MODE);
            // 获取文件当前的长度，用于定位写入位置到文件末尾。
            len = file.length();
            // 将写入位置定位到文件末尾，准备写入整数。
            file.seek(len);
            // 写入整数值到文件中。
            file.writeInt(value);
            // 关闭文件流。
            file.close();
        } catch (Exception e) {
            // 捕获并打印任何异常，确保资源的释放和错误的记录。
            e.printStackTrace();
        }
    }


    /**
     * 根据索引和长度从文件中读取字节数据。
     * 该方法通过RandomAccessFile实现对文件的随机读取，可以从指定的索引位置开始读取指定长度的数据。
     *
     * @param filePath 文件路径，指定要读取的文件。
     * @param index    读取的起始索引位置，单位为字节。
     * @param len      读取的长度，指定要读取的字节数量。
     * @return 返回读取的字节数据，如果发生异常则返回null。
     */
    public static byte[] readByIndex(String filePath, int index, int len) {
        RandomAccessFile file = null;
        byte[] res = new byte[len];
        try {
            // 使用RandomAccessFile打开文件，以读写模式（"rw"）访问。
            file = new RandomAccessFile(filePath, RW_MODE);
            // 将文件指针定位到指定的索引位置。
            file.seek((long)index);
            // 从当前位置开始读取指定长度的字节到res数组中。
            file.read(res, 0, len);
            return res;
        } catch (Exception e) {
            // 捕获并打印任何发生的异常。
            e.printStackTrace();
        } finally {
            try {
                // 确保在方法退出时关闭文件。
                if (file != null) {
                    file.close();
                }
            } catch (IOException e) {
                // 捕获并打印文件关闭过程中发生的异常。
                e.printStackTrace();
            }
        }
        // 如果发生异常，返回null。
        return null;
    }


}
