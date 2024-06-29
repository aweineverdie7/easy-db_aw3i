package utils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

}
