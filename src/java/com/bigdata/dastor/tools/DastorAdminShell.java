package com.bigdata.dastor.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.commons.cli.ParseException;

public class DastorAdminShell
{
    private static void printUsage()
    {
        DastorAdminCli.printUsage();
        System.out.println("\n-- Utility commands:");
        System.out.println(" quit/edit    - quit/exit form this shell.");
        System.out.println(" help/?       - print usage of commands.");
        System.out.println(" connect      - switch the connection to a new node: <host> <port>");
        System.out.println("\n");
    }
    
    private static void printLogo()
    {
        System.out.println("  _____________        _________      _____        ");
        System.out.println("  ___  __ )__(_)______ ______  /_____ __  /______ _");
        System.out.println("  __  __  |_  /__  __ `/  __  /_  __ `/  __/  __ `/");
        System.out.println("  _  /_/ /_  / _  /_/ // /_/ / / /_/ // /_ / /_/ / ");
        System.out.println("  /_____/ /_/  _\\__, / \\__,_/  \\__,_/ \\__/ \\__,_/  ");
        System.out.println("               /____/     DaStor System            ");
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ParseException
    {
        ArrayList<String> cmdArgList = new ArrayList<String>();
        DastorAdminCli nodeCmd = DastorAdminCli.initCli(args, cmdArgList);
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        System.out.println("\n");
        printLogo();
        
        while(true) {
            System.out.println("\n");
            System.out.print("DastorAdmin->");
            String cmd = reader.readLine();
            cmd = cmd.trim();
            
            if (cmd.equalsIgnoreCase("help") || cmd.equalsIgnoreCase("?"))
            {
                printUsage();
                continue;
            }
            else if (cmd.equalsIgnoreCase("quit") || cmd.equalsIgnoreCase("exit"))
            {
                nodeCmd.closeProbe();
                System.out.println("Quit!");
                break;
            }
            else if (cmd.equals(""))
            {
                continue;
            }
            
            // delimiter is one(more) whitespace(s)
            String[] cmdArgs = cmd.split("\\s+");
            
            if (cmdArgs.length > 0)
            {
                if (cmdArgs[0].equals("connect"))
                {
                    if (cmdArgs.length < 2)
                    {
                        System.err.println("Missing remote node(JMX agent) host (& port) arguments!\n");
                        printUsage();
                        continue;
                    }
                    String host = cmdArgs[1];
                    int port = nodeCmd.getPort();
                    if (cmdArgs.length >= 3)
                    {
                        try
                        {
                            port = Integer.parseInt(cmdArgs[2]);
                        }
                        catch (NumberFormatException e)
                        {
                            System.err.println("The remote node(JMX agent) port must be a integer!\n");
                            continue;
                        }
                    }
                    
                    System.out.println("Closing connection to " + nodeCmd.getHost() + ":" + nodeCmd.getPort());
                    nodeCmd.closeProbe();
                    
                    NodeProbe probe = null;
                    try
                    {
                        probe = new NodeProbe(host, port);
                    }
                    catch (IOException ioe)
                    {
                        System.err.println("Error connecting to node(JMX agent)!");
                        ioe.printStackTrace();
                        continue;                        
                    }
                    nodeCmd.setProbe(probe, host, port);
                    System.out.println("Connected to " + host + ":" + port);
                    continue;
                }
            }

            if (nodeCmd.getProbe() == null)
            {
                System.out.println("Please connect to a node(JMX agent) firstly!");
                continue;
            }
            
            int ret = 0;
            try
            {
                ret = nodeCmd.executeCommand(cmdArgs);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            
            System.out.println("Command execute result = " + ret);
        }
        
        System.exit(0);
    }
    
}
