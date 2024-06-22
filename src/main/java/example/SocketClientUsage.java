/*
 *@Type SocketClientUsage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 14:07
 * @version
 */
package example;

import client.Client;
import client.SocketClient;

public class SocketClientUsage {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 12345;
        Client client = new SocketClient(host, port);
//        client.get("zsy1");
        client.set("key1","value1");
        client.set("key2","value2");
        client.set("key3","value3");

//        client.get("zsy12");
//        client.rm("zsy12");
//        client.get("zsy12");
    }
}