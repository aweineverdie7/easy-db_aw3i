package example;

import client.Client;
import client.EasydbCli;
import client.SocketClient;

public class CmdClientUsage {
    public static void main(String[] args) {
        EasydbCli cli = new EasydbCli();
        cli.startInteractiveSession(args);
    }
}