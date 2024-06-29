package utils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionUtils {
    // 创建一个线程池，用于执行压缩任务
    private static final ExecutorService compressionExecutor = Executors.newFixedThreadPool(8);

    /**
     * 对指定文件进行GZIP压缩，并可选地删除原始文件。
     *
     * @param originalFilePath 需要压缩的文件路径。
     * @param deleteOriginal 是否在压缩成功后删除原始文件。
     * @throws IOException 如果文件读写过程中发生错误。
     */
    public static void compressFile(String originalFilePath, boolean deleteOriginal) throws IOException {
        // 拼接压缩后的文件路径
        String compressedFilePath = originalFilePath + ".gz";

        try (
                FileInputStream fis = new FileInputStream(originalFilePath);
                FileOutputStream fos = new FileOutputStream(compressedFilePath);
                GZIPOutputStream gzipOS = new GZIPOutputStream(fos)
        ) {
            byte[] buffer = new byte[1024];
            int read;
            while ((read = fis.read(buffer)) != -1) {
                gzipOS.write(buffer, 0, read);
            }
        } finally {
            if (deleteOriginal) {
                Files.delete(Paths.get(originalFilePath));
            }
        }
    }
        /**
     * 异步压缩文件。
     * 使用线程池中的线程执行文件压缩操作，以异步方式提高程序的并发性能。
     * 当文件压缩操作完成时，不需要等待压缩过程结束，可以立即返回并继续执行后续代码。
     *
     * @param filePath 要压缩的文件路径。
     */
    // 异步压缩文件
    public static void compressFileAsync(String filePath,boolean deleteOriginal) {
        compressionExecutor.submit(() -> {
            try {
                compressFile(filePath,deleteOriginal);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
    /**
     * 解压缩GZIP文件到指定路径或删除.gz后缀还原原文件名。
     *
     * @param compressedFilePath 压缩文件路径。
     * @param destinationPath   解压后文件存放路径，如果不指定则默认解压到与压缩文件同目录并去除.gz后缀。
     * @throws IOException 如果文件读写过程中发生错误。
     */
    public static void decompressFile(String compressedFilePath, String destinationPath) throws IOException {
        Path source = Paths.get(compressedFilePath);
        Path target = destinationPath == null ?
                source.getParent().resolve(source.getFileName().toString().replace(".gz", "")) :
                Paths.get(destinationPath);

        try (
                FileInputStream fis = new FileInputStream(compressedFilePath);
                GZIPInputStream gzipIS = new GZIPInputStream(fis);
                FileOutputStream fos = new FileOutputStream(target.toFile())
        ) {
            byte[] buffer = new byte[1024];
            int read;
            while ((read = gzipIS.read(buffer)) != -1) {
                fos.write(buffer, 0, read);
            }
        }
    }
    /**
     * 异步解压缩文件。
     * 使用线程池中的线程执行文件解压缩操作。
     *
     * @param compressedFilePath 压缩文件路径。
     * @param destinationPath 解压后文件存放路径。
     */
    public static void decompressFileAsync(String compressedFilePath, String destinationPath) {
        compressionExecutor.submit(() -> {
            try {
                decompressFile(compressedFilePath, destinationPath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
    /**
     * 从GZ压缩文件中解压指定位置和长度的数据。
     *
     * 此方法用于从GZIP格式的压缩文件中提取指定位置开始的特定长度的数据。
     * 它首先打开文件输入流和GZIP输入流，然后跳转到指定的位置，接着读取指定长度的数据，
     * 并将这些数据写入 ByteArrayOutputStream 中，最后将这些数据作为字节数组返回。
     *
     * @param filePath GZ压缩文件的路径。
     * @param pos 从文件中的哪个位置开始读取数据。
     * @param len 要读取的数据长度。
     * @return 从文件中解压出的字节数组。
     * @throws IOException 如果读取或解压过程中发生错误。
     */
    public static byte[] decompressGZFile(String filePath, long pos, int len) throws IOException {
        // 使用 try-with-resources 确保资源正确关闭
        try (FileInputStream fis = new FileInputStream(filePath);
             GZIPInputStream gzipIn = new GZIPInputStream(fis);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            // 跳转到指定的位置以开始读取数据
            // 跳到指定位置
            fis.skip(pos);

            // 创建一个缓冲区用于存储从压缩文件中读取的数据
            byte[] buffer = new byte[len];
            int bytesRead;
            // 读取数据直到没有更多数据可读
            while ((bytesRead = gzipIn.read(buffer)) > 0) {
                // 将读取到的数据写入 ByteArrayOutputStream
                baos.write(buffer, 0, bytesRead);
            }
            // 返回解压后的数据作为字节数组
            return baos.toByteArray();
        }
    }

}
