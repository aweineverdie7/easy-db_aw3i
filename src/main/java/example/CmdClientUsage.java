package example;

import client.Client;
import client.CmdClient;
import client.SocketClient;

public class CmdClientUsage {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 12345;
        Client client = new SocketClient(host, port);
        CmdClient cli = new CmdClient(client);
        cli.parseArguments(args);
    }
}