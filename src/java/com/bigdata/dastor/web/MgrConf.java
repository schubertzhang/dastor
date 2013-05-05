package com.bigdata.dastor.web;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import com.bigdata.dastor.utils.XMLUtils;

public class MgrConf 
{
    
    private static Logger logger = Logger.getLogger(MgrConf.class);

    private static final String confFileName = "cluster.xml";
    
    private String clusterName;
    private List<String> nodeList;
    
    public MgrConf()
    {
        URL url = MgrConf.class.getClassLoader().getResource(confFileName);
        
        if (url == null)
        {
            logger.error(confFileName + " : Load Error: not exist.");
            System.exit(-1);
        }
        
        File file = new File(url.getFile());

        if (file.exists())
        {
            logger.info(confFileName + " : " + file.getAbsolutePath());
        }
        else 
        {
            logger.error(confFileName + " : Load Error: not exist.");
            System.exit(-1);
        }
        
        try 
        {
            XMLUtils xmlUtils = new XMLUtils(file.getAbsolutePath());
            clusterName = xmlUtils.getNodeValue("/dastor/cluster/name");
            String[] nodes = xmlUtils.getNodeValues("/dastor/cluster/node_list/node");
            nodeList = new ArrayList<String>();
            for (String node : nodes)
                nodeList.add(node);
        } 
        catch (FileNotFoundException e) 
        {
            e.printStackTrace();
            logger.equals(confFileName + " : Load Error: "+e);
            System.exit(-1);
        } 
        catch (ParserConfigurationException e) 
        {
            e.printStackTrace();
            logger.equals(confFileName + " : Load Error: "+e);
            System.exit(-1);
        } 
        catch (SAXException e) 
        {
            e.printStackTrace();
            logger.equals(confFileName + " : Load Error: "+e);
            System.exit(-1);
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
            logger.equals(confFileName + " : Load Error: "+e);
            System.exit(-1);
        } 
        catch (XPathExpressionException e) 
        {
            e.printStackTrace();
            logger.equals(confFileName + " : Load Error: "+e);
            System.exit(-1);
        }
    }
    
    public String getClusterName()
    {
        return clusterName;
    }
    
    public List<String> getNodeList()
    {
        
        return nodeList;
    }

}
