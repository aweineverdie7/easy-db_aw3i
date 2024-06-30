package client;

import org.apache.commons.cli.*;

import java.util.Scanner;

/**
 * 类EasydbCli模拟了一个简化版的kv数据库命令行客户端，
 * 支持通过命令行指定主机和端口登录，然后在交互式界面中执行数据库操作。
 */
public class EasydbCli {
    private SocketClient client;

    public EasydbCli() {

    }

    /**
     * 开始交互式命令行会话。
     * 首先解析登录参数(-h 主机 -p 端口)，然后进入命令处理循环。
     *
     * @param args 登录参数数组
     */
    public void startInteractiveSession(String[] args) {
        Options loginOptions = new Options();

        // 添加主机地址选项
        Option hostOption = new Option("h", "host", true, "数据库服务器的主机名或IP地址");
        hostOption.setRequired(true);
        loginOptions.addOption(hostOption);

        // 添加端口选项，默认为12345（Redis默认端口）
        Option portOption = new Option("p", "port", true, "数据库服务端口");
        portOption.setRequired(false);
        portOption.setArgName("端口号");
        loginOptions.addOption(portOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(loginOptions, args);

            String host = cmd.getOptionValue("host");
            String portStr = cmd.getOptionValue("port");
            int port = Integer.parseInt(portStr);

            // 尝试连接到数据库服务器
            client = new SocketClient(host, port);
            if (!((SocketClient) client).canConnectToServer()) {
                System.err.println("无法连接到数据库服务器。");
                return;
            }

            // 成功连接后，启动交互式命令行循环
            interactiveCommandLoop();

        } catch (ParseException | NumberFormatException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("EasydbCli", loginOptions);
        }
    }


    /**
     * 运行交互式命令行循环，处理用户输入的命令。
     */
    private void interactiveCommandLoop() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入命令（如'set key value'，'get key'，'rm key' 或 'exit'退出）：");

        while (true) {
            System.out.print("> ");
            String input = scanner.nextLine().trim().toLowerCase();

                handleCommand(input.split(" "));

        }
    }

    /**
     * 处理用户输入的命令。
     *
     * @param commandParts 分割后的命令字符串数组
     */
private void handleCommand(String[] commandParts) {
    switch (commandParts[0]) {
        case "set":
            if (commandParts.length == 3) {
                client.set(commandParts[1], commandParts[2]);
                System.out.println("键 \"" + commandParts[1] + "\" 的值已设置为 \"" + commandParts[2] + "\"。");
            } else {
                System.err.println("使用方法: set <key> <value>");
            }
            break;
        case "get":
            if (commandParts.length == 2) {
                String value = client.get(commandParts[1]);
                if (value != null) {
                    System.out.println("键 \"" + commandParts[1] + "\" 的值为: " + value);
                } else {
                    System.out.println("键 \"" + commandParts[1] + "\" 未找到。");
                }
            } else {
                System.err.println("使用方法: get <key>");
            }
            break;
        case "rm":
            if (commandParts.length == 2) {
//                boolean isRemoved = client.rm(commandParts[1]);
//                if (isRemoved) {
                    client.rm(commandParts[1]);
                    System.out.println("键 \"" + commandParts[1] + "\" 已被移除。");
//                } else {
//                    System.out.println("键 \"" + commandParts[1] + "\" 不存在，无法移除。");
//                }
            } else {
                System.err.println("使用方法: rm <key>");
            }
            break;
        case "exit":
            System.out.println("退出程序...");
            client.exit();
            System.exit(0); // 优雅地退出程序
            break;
        default:
            System.err.println("未知命令: " + commandParts[0] + ". 请输入 'help' 查看可用命令。");
    }
}

}
