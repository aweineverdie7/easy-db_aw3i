/*
 *@文件类型: SocketServerHandler.java
 *@描述: 实现Socket服务器端处理逻辑的线程类
 *@作者: urmsone
 *@邮箱: urmsone@163.com
 *@日期: 2024/6/13 12:50
 *@版本: 版本信息
 */

package controller; // 包声明

import dto.ActionDTO; // 动作数据传输对象
import dto.ActionTypeEnum; // 动作类型枚举
import dto.RespDTO; // 响应数据传输对象
import dto.RespStatusTypeEnum; // 响应状态类型枚举
import service.NormalStore; // 示例存储服务
import service.Store; // 存储接口
import utils.LoggerUtil; // 日志工具类

import java.io.*; // 输入输出相关包
import java.net.Socket; // Socket通信类
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger; // SLF4J日志框架的Logger接口
import org.slf4j.LoggerFactory; // SLF4J工厂类，用于获取Logger实例

// 实现Runnable接口，以便在独立线程中运行
public class SocketServerHandler implements Runnable {
    private final Logger LOGGER = LoggerFactory.getLogger(SocketServerHandler.class); // 初始化日志记录器
    private Socket socket; // 客户端Socket连接
    private Store store; // 数据存储服务实例

    // 构造函数，接收Socket连接和存储服务实例
    public SocketServerHandler(Socket socket, Store store) {
        this.socket = socket;
        this.store = store;
    }

    // 当线程执行时调用此方法
    @Override
    public void run() {
        try (
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream()); // 输入流，用于读取客户端发送的数据
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream()) // 输出流，用于向客户端发送数据
        ) {
            // 从输入流中读取序列化的ActionDTO对象
            ActionDTO dto = (ActionDTO) ois.readObject();
            LoggerUtil.debug(LOGGER, "[SocketServerHandler][ActionDTO]: {}", dto.toString()); // 记录接收到的ActionDTO日志

            // 动态处理不同类型的命令（策略模式）
            handleCommand(dto, oos);

        } catch (IOException | ClassNotFoundException e) { // 捕获IO异常和类找不到异常
            e.printStackTrace(); // 打印堆栈信息
        } finally {
            try {
                socket.close(); // 确保Socket连接关闭
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // 处理命令逻辑的方法，采用策略模式提高扩展性
    private void handleCommand(ActionDTO dto, ObjectOutputStream oos) throws IOException {
        // 这里可以根据需求定义更多命令处理器，并注册到map中
        Map<ActionTypeEnum, CommandHandler> handlers = new HashMap<>();
        handlers.put(ActionTypeEnum.CONNET, this::handleConnet);
        handlers.put(ActionTypeEnum.GET, this::handleGet);
        handlers.put(ActionTypeEnum.SET, this::handleSet);
        handlers.put(ActionTypeEnum.RM, this::handleRemove);
        handlers.put(ActionTypeEnum.EXIT, this::handleExit);

        CommandHandler handler = handlers.get(dto.getType());
        if (handler != null) {
            handler.handle(dto, oos);
        } else {
            // 如果没有找到对应的处理器，则返回错误响应
            sendErrorResponse(oos, "未知的命令类型");
        }
    }

    private void handleConnet(ActionDTO actionDTO, ObjectOutputStream objectOutputStream) throws IOException {
    // 对于连接确认命令，我们可以简单地回复一个成功的响应，
    // 表明服务器已经收到了该请求并且连接是活跃的。心跳机制
    RespDTO resp = new RespDTO(RespStatusTypeEnum.SUCCESS, "连接成功");
    objectOutputStream.writeObject(resp);
    objectOutputStream.flush();
    LoggerUtil.info(LOGGER, "[SocketServerHandler][handleConnet]: 客户端连接确认响应已发送。");
}


    // 处理GET命令的逻辑
    private void handleGet(ActionDTO dto, ObjectOutputStream oos) throws IOException {
        String value = this.store.get(dto.getKey());
        LoggerUtil.debug(LOGGER, "[SocketServerHandler][run]: {}", "get action resp" + dto.toString());
        sendSuccessResponse(oos, value);
    }

    // 处理SET命令的逻辑
    private void handleSet(ActionDTO dto, ObjectOutputStream oos) throws IOException {
        this.store.set(dto.getKey(), dto.getValue());
        LoggerUtil.debug(LOGGER, "[SocketServerHandler][run]: {}", "set action resp" + dto.toString());
        sendSuccessResponse(oos, null);
    }

    // 处理RM命令的逻辑
    private void handleRemove(ActionDTO dto, ObjectOutputStream oos) throws IOException {
        this.store.rm(dto.getKey());
        LoggerUtil.debug(LOGGER, "[SocketServerHandler][run]: {}", "rm action resp" + dto.toString());
        sendSuccessResponse(oos, null);
    }

    // 处理EXIT命令的逻辑
    private void handleExit(ActionDTO dto, ObjectOutputStream oos) throws IOException {
        this.store.close();
        LoggerUtil.debug(LOGGER, "[SocketServerHandler][run]: {}", "exit action resp" + dto.toString());
        sendSuccessResponse(oos, null);
    }

    // 发送成功响应的方法
    private void sendSuccessResponse(ObjectOutputStream oos, String value) throws IOException {
        RespDTO resp = new RespDTO(RespStatusTypeEnum.SUCCESS, value);
        oos.writeObject(resp);
        oos.flush();
    }

    // 发送错误响应的方法
    private void sendErrorResponse(ObjectOutputStream oos, String errorMessage) throws IOException {
        RespDTO resp = new RespDTO(RespStatusTypeEnum.FAIL, errorMessage);
        oos.writeObject(resp);
        oos.flush();
    }

    // 定义命令处理器接口
    interface CommandHandler {
        void handle(ActionDTO dto, ObjectOutputStream oos) throws IOException;
    }
}
