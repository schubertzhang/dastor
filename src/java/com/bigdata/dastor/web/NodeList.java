package com.bigdata.dastor.web;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.bigdata.dastor.tools.NodeProbe;

public class NodeList 
{

    private static NodeList instance;
    
    private String clusterName;
    
    private HashMap<String, NodeProbe> liveMap;
    private HashMap<String, NodeProbe> deadMap;
    
    private HashMap<String, Integer> portMap;
    
    private NodeList()
    {
        liveMap = new HashMap<String, NodeProbe>();
        deadMap = new HashMap<String, NodeProbe>();
        portMap = new HashMap<String, Integer>();
        
        MgrConf conf = new MgrConf();
        clusterName = conf.getClusterName();
        List<String> nodeList = conf.getNodeList();
        for (String node : nodeList)
        {
            try
            {
                String[] ip_port = node.split(":");
                deadMap.put(ip_port[0], null);
                if (ip_port.length == 2)
                    portMap.put(ip_port[0], Integer.parseInt(ip_port[1]));
                else
                    portMap.put(ip_port[0], 8081);
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
        connectAllDeadNode();
    }
    
    public static synchronized NodeList getInstance()
    {
        if (instance == null)
            instance = new NodeList();
        return instance;
    }
    
    public void setNodeDead(String node)
    {
        liveMap.get(node).close();
        liveMap.remove(node);
        deadMap.put(node, null);
    }
    
    public void connectAllDeadNode()
    {
        Set<String> nodeList = deadMap.keySet();
        List<String> liveList = new ArrayList<String>();
        
        for (String node : nodeList)
        {
            try {
                NodeProbe nodeProbe = new NodeProbe(node, portMap.get(node));
                liveList.add(node);
                liveMap.put(node, nodeProbe);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        for (String node : liveList)
        {
            deadMap.remove(node);
        }
    }
    
    public NodeProbe getNodeProbe(String node)
    {
        if (liveMap.containsKey(node))
            return liveMap.get(node);
        
        return null;
    }
    
    public HashMap<String, NodeProbe> getLiveMap()
    {
        return liveMap;
    }
    
    public HashMap<String, NodeProbe> getDeadMap()
    {
        return deadMap;
    }
    
    public String getClusterName()
    {
        return clusterName;
    }
}
