package client;

import org.apache.commons.cli.*;

public class CmdClient{
    private final Client client ;

    public CmdClient(Client client) {
        this.client = client;
    }

        /**
     * 解析命令行参数并执行相应操作。
     * 此方法支持设置键值对、获取键的值以及移除键的操作。
     * 它使用Apache Commons CLI库来处理命令行选项。
     *
     * @param args 命令行参数
     */
    public void parseArguments(String[] args) {
        // 初始化选项配置，包含设置、获取和移除三个选项
        Options options = new Options();

        // 配置设置选项，需要两个参数：键和值
        Option set = new Option("s", "set", true, "设置键值对");
        set.setArgs(2);
        options.addOption(set);

        // 配置获取选项，需要一个参数：键
        Option get = new Option("g", "get", true, "获取键的值");
        get.setArgs(1);
        options.addOption(get);

        // 配置移除选项，需要一个参数：键
        Option rm = new Option("r", "rm", true, "移除键");
        rm.setArgs(1);
        options.addOption(rm);

        // 初始化命令行解析器和帮助信息格式化器
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            // 解析命令行参数
            CommandLine cmd = parser.parse(options, args);

            // 根据提供的选项执行相应操作
            if (cmd.hasOption("s")) {
                String[] values = cmd.getOptionValues("s");
                client.set(values[0], values[1]);
            } else if (cmd.hasOption("g")) {
                String key = cmd.getOptionValue("g");
                System.out.println("值: " + client.get(key));
            } else if (cmd.hasOption("r")) {
                String key = cmd.getOptionValue("r");
                client.rm(key);
            } else {
                // 如果没有提供有效选项，则显示帮助信息
                formatter.printHelp("CmdClient", options);
            }
        } catch (ParseException e) {
            // 如果解析命令行时发生错误，打印错误信息并显示帮助信息
            System.out.println(e.getMessage());
            formatter.printHelp("CmdClient", options);
        }
    }

}