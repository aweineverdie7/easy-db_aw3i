/*
 *@Type SocketClient.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 13:15
 * @version
 */
package client;

import dto.ActionDTO;
import dto.ActionTypeEnum;
import dto.RespDTO;

import java.io.*;
import java.net.Socket;

public class SocketClient implements Client {
    private String host;
    private int port;

    public SocketClient(String host, int port) {
        this.host = host;
        this.port = port;
    }
    public boolean canConnectToServer() {
        try (Socket socket = new Socket(host, port)) {
            // 连接成功，不执行任何读写操作，直接返回true
            return true;
        } catch (IOException e) {
            // 连接失败，打印异常信息（可选）
            System.err.println("Failed to connect to server: " + e.getMessage());
            return false;
        }
    }

    @Override
    public void set(String key, String value) {
        try (Socket socket = new Socket(host, port);
             ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {
            // 传输序列化对象
            ActionDTO dto = new ActionDTO(ActionTypeEnum.SET, key, value);
            oos.writeObject(dto);
            oos.flush();
            RespDTO resp = (RespDTO) ois.readObject();
            System.out.println("resp data: "+ resp.toString());
            // 接收响应数据
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String get(String key) {
        try (Socket socket = new Socket(host, port);
             ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {
            // 传输序列化对象
            ActionDTO dto = new ActionDTO(ActionTypeEnum.GET, key, null);
            oos.writeObject(dto);
            oos.flush();
            RespDTO resp = (RespDTO) ois.readObject();
            System.out.println("resp data: "+ resp.toString());
            // 接收响应数据
            return resp.getValue();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void rm(String key) {

    }

}
