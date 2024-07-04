package test;


import service.NormalStore;
import java.io.File;
import java.util.Random;

public class LargeDataTest {

    private static final int KB = 1024; // 1KB大小
    private static final int TARGET_SIZE_MB = 10; // 目标数据量为10MB
    private static final int TOTAL_PAIRS = TARGET_SIZE_MB * KB;

    public static void main(String[] args) {
        String dataDir = "large_data_test" + File.separator;
        NormalStore store = new NormalStore(dataDir);

        System.out.println("准备写入 " + TOTAL_PAIRS + " 对键值数据，总数据量超过 " + TARGET_SIZE_MB + "MB...");

        // 生成随机数据
        Random random = new Random();
        StringBuilder valueBuilder = new StringBuilder(KB); // 用于生成1KB大小的数据值

        for (int i = 1; i <= TOTAL_PAIRS; i++) {
            // 生成1KB长度的随机字符串作为值
            for (int j = 0; j < KB; j++) {
                valueBuilder.append((char) ('a' + random.nextInt(26)));
            }
            String value = valueBuilder.toString(); // 重置StringBuilder以便下一次迭代
            valueBuilder.setLength(0);

            String key = "key_" + String.format("%05d", i); // 键的格式形如 'key_00001'
            store.set(key, value);

            // 定期打印进度（每10%打印一次）
            if (i % (TOTAL_PAIRS / 10) == 0) {
                System.out.println("进度: " + (i / (double) TOTAL_PAIRS * 100) + "%");
            }
        }
        store.set("awei", "202210244317");

        System.out.println("数据写入完成。");
        // 验证get操作
        System.out.println("验证读取操作...");
        int testKeyIndex = 10101; // 选择中间的一个键进行测试
        String testKey = "key_" + String.format("%05d", testKeyIndex);
        String retrievedValue = store.get(testKey);
        System.out.println("键 '" + testKey + "' 的值为: " + retrievedValue);
        String awei = store.get("awei");
        System.out.println("键 'awei' 的值为: " + awei);
        // 测试rm操作并再次get以验证删除
        System.out.println("测试删除操作...");
        String keyToRemove = "key_" + String.format("%05d", testKeyIndex);
        store.rm(keyToRemove);
        store.rm("awei");
            System.out.println("键 '" + keyToRemove + "' 待验证删除。");
            // 尝试再次获取已删除的键，预期结果应为null
            String valueAfterRemove = store.get(keyToRemove);
            String aweiAfterRemove = store.get("awei");
            if (aweiAfterRemove == null) {
                System.out.println("验证通过: 删除后的键 'awei' 无法获取。");
            } else {
                System.out.println("错误: 删除后的键 'awei' 仍可获取。");
            }
            if (valueAfterRemove == null) {
                System.out.println("验证通过: 删除后的键 '" + keyToRemove + "' 无法获取。");
            } else {
                System.out.println("错误: 删除后的键仍可获取。");
            }

    }
}

