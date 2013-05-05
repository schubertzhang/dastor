package com.bigdata.dastor.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.bigdata.dastor.io.util.FileUtils;
import com.bigdata.dastor.tools.NodeProbe;
import com.bigdata.dastor.utils.DiskSpaceLoad;

public class ClusterServlet extends HttpServlet{

    private static final long serialVersionUID = -411290031406666049L;

    private static Logger logger = Logger.getLogger(ClusterServlet.class);

    /* (non-Javadoc)
     * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
     */
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException 
    {
        String op = req.getParameter("op");
        String ret = null;
        if (op == null)
        {
            logger.error("op = null");
            Result.R r = new Result.R();
            r.buildError("op == null");
            ObjectMapper mapper = new ObjectMapper();
            ret = mapper.writeValueAsString(r);
        }
        else if (op.equals("cluster_name"))
        {
            logger.info("get cluster name");
            ret = procClusterName();
        }
        else if (op.equals("node_list"))
        {
            NodeList.getInstance().connectAllDeadNode();
            logger.info("get node list");
            ret = procNodeList();
        }
        else
        {
            logger.error("error op : " + op);
            Result.R r = new Result.R();
            r.buildError("error op : " + op);
            ObjectMapper mapper = new ObjectMapper();
            ret = mapper.writeValueAsString(r);
        }
        
        logger.info("ret : "+ret);
        PrintWriter writer = resp.getWriter();
        writer.println(ret);
        writer.close();
    }

    /* (non-Javadoc)
     * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
     */
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException 
    {
        doGet(req, resp);
    }

    private String procClusterName() throws JsonGenerationException, JsonMappingException, IOException
    {
        Result.ClusterName r = new Result.ClusterName();

        String name = NodeList.getInstance().getClusterName();
        if (name == null)
        {
            r.buildError("get error cluster name");
        }
        else
        {
            r.buildOK();
            r.setName(name);
        }
        
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(r);
    }

    private String procNodeList() throws JsonGenerationException, JsonMappingException, IOException
    {
        Result.NodeInfoList r = new Result.NodeInfoList();

        // check live nodes
        HashMap<String, NodeProbe> liveMap = NodeList.getInstance()
                .getLiveMap();
        ArrayList<String> toDelList = new ArrayList<String>();

        for (Map.Entry<String, NodeProbe> entry : liveMap.entrySet()) {
            NodeProbe probe = entry.getValue();
            try {
                if (probe.getOperationMode() == null
                        || probe.getCurrentGenerationNumber() == 0) {
                    toDelList.add(entry.getKey());
                }
            } catch (Exception e) {
                toDelList.add(entry.getKey());
            }
        }
        for (String deadNode : toDelList)
            NodeList.getInstance().setNodeDead(deadNode);

        // connect all dead node
        NodeList.getInstance().connectAllDeadNode();
        
        liveMap = NodeList.getInstance().getLiveMap();
        
        ArrayList<Result.NodeInfo> liveList = new ArrayList<Result.NodeInfo>();
        
        toDelList = new ArrayList<String>();
        for (Map.Entry<String, NodeProbe> entry : liveMap.entrySet())
        {
            Result.NodeInfo info = new Result.NodeInfo();
            info.setIp(entry.getKey());
            info.setAlive(true);
            
            try
            {
                NodeProbe probe = entry.getValue();

                info.setOperating(probe.getOperationMode());
                info.setGeneration(probe.getCurrentGenerationNumber());
                info.setUptime(getUptimeTimeString(probe.getUptime()));
                
                HashMap<String, Double> diskMap = new HashMap<String, Double>();
                for (FileUtils.FSInfo fsinfo : probe.getStorageFSInfo().values())
                {
                    diskMap.put(fsinfo.fsName, fsinfo.totalSize);
                }
                double s = 0;
                for (Double size : diskMap.values())
                {
                    s += size.doubleValue();
                }
                info.setConfiguredCapacity(FileUtils.stringifyFileSize(s));
                
                DiskSpaceLoad dsl = probe.getStorageServiceMBean().getDiskSpaceLoad();
                info.setLoad(FileUtils.stringifyFileSize(dsl.net));
                info.setGrossLoad(FileUtils.stringifyFileSize(dsl.gross));
                
                MemoryUsage heapUsage = probe.getHeapMemoryUsage();
                double memUsed = (double)heapUsage.getUsed() / (1024 * 1024);
                double memMax = (double)heapUsage.getMax() / (1024 * 1024);
                info.setHeapusage(String.format("%.2fMB/%.2fMB", memUsed, memMax));
            }
            catch(Exception e)
            {
                if (e instanceof IOException)
                    toDelList.add(entry.getKey());
            }
            liveList.add(info);
        }
        for (String deadNode : toDelList)
           NodeList.getInstance().setNodeDead(deadNode);
        
        HashMap<String, NodeProbe> deadMap = NodeList.getInstance().getDeadMap();

        ArrayList<Result.NodeInfo> deadList = new ArrayList<Result.NodeInfo>();
        for (Map.Entry<String, NodeProbe> entry : deadMap.entrySet())
        {
            Result.NodeInfo info = new Result.NodeInfo();
            info.setIp(entry.getKey());
            info.setAlive(false);

            deadList.add(info);
        }
        
        r.buildOK();
        r.setLiveList(liveList);
        r.setDeadList(deadList);
        
        ObjectMapper mapper = new ObjectMapper();
        
        return mapper.writeValueAsString(r);
    }
    
    public static String getUptimeTimeString(long time){
        
        long day = time / (1000*60*60*24);
        long tmp = time % (1000*60*60*24);
        long hour = tmp / (1000*60*60);
        tmp = tmp % (1000*60*60);
        long min = tmp / (1000*60);
        tmp = tmp % (1000*60);
        long sec = tmp / 1000;
        
        if (day > 0) 
        {
            return day + " day " + hour + " hour " + min + " min " + sec
                    + " sec";
        }
        else if (hour > 0)
        {
            return hour + " hour " + min + " min " + sec + " sec";
        }
        else if (min > 0)
        {
            return min + " min " + sec + " sec";
        }
        else
        {
            return sec + " sec";
        }
    }
}
