package com.bigdata.dastor.cfc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import com.bigdata.dastor.config.DatabaseDescriptor;
import com.bigdata.dastor.db.IColumn;
import com.bigdata.dastor.db.ReadCommand;
import com.bigdata.dastor.db.Row;
import com.bigdata.dastor.db.RowMutation;
import com.bigdata.dastor.db.SliceByNamesReadCommand;
import com.bigdata.dastor.db.SliceFromReadCommand;
import com.bigdata.dastor.db.SystemTable;
import com.bigdata.dastor.db.filter.QueryPath;
import com.bigdata.dastor.service.StorageProxy;
import com.bigdata.dastor.thrift.ConsistencyLevel;
import com.bigdata.dastor.thrift.InvalidRequestException;
import com.bigdata.dastor.thrift.UnavailableException;
import com.bigdata.dastor.utils.FBUtilities;

/**
 *  Column Family Collector (CFC).
 *  The collection task will run periodically, to check the reset operations on CFs 
 *  from cluster system table (ClsSystem), and reset these requested CFs.
 */
public class Collector
{
    private static final Logger logger = Logger.getLogger(Collector.class);
    
    // cluster system table schema for CFC.
    public static final String ClsSystem_KS = "ClsSystem";
    public static final String CFC_CF = "CFC";
    public static final String CFC_ResetOp_KEY = "ResetOp";
    // there is only one row(key), and the style of column names likes "ks:cf".
    
    // default time-segment within a day to run collection task periodically.
    public static final String Default_Begin_Time = "2:00";
    public static final String Default_End_Time   = "4:00";
    
    public static void initTask()
    {
        if (DatabaseDescriptor.getCFMetaData(ClsSystem_KS, CFC_CF) == null)
        {
            logger.warn("No config of cluster system table or CFC CF, won't start CFC: "  
                    + ClsSystem_KS + "," + CFC_CF);
            return;
        }
        
        String cfgBeginTime = DatabaseDescriptor.getCFCBeginTime();
        String cfgEndTime = DatabaseDescriptor.getCFCEndTime();
        
        if (!checkTimeFormat(cfgBeginTime))
        {
            logger.warn("No valid config of CFCBeginTime, use default: " + Default_Begin_Time);
            cfgBeginTime = Default_Begin_Time;
        }
        if (!checkTimeFormat(cfgEndTime))
        {
            logger.warn("No valid config of CFCEndTime, use default: " + Default_End_Time);
            cfgEndTime = Default_End_Time;
        }
        
        long curTimeMs = System.currentTimeMillis();
        Calendar beginTimeCalender = getCalendar(cfgBeginTime, curTimeMs);
        Calendar endTimeCalender = getCalendar(cfgEndTime, curTimeMs);

        // if EndTime <= BeginTime, it means the EndTime is in the next day.
        if (endTimeCalender.getTimeInMillis() <=  beginTimeCalender.getTimeInMillis())
        {
            endTimeCalender.setTimeInMillis(endTimeCalender.getTimeInMillis()+CollectorTimerTask.ONE_DAY_IN_MS);
        }
        
        long runningTime = endTimeCalender.getTimeInMillis() - beginTimeCalender.getTimeInMillis();
        
        // if the current time is later than the begin time, will begin in next day.
        if (System.currentTimeMillis() >= beginTimeCalender.getTimeInMillis())
        {
            beginTimeCalender.setTimeInMillis(beginTimeCalender.getTimeInMillis()+CollectorTimerTask.ONE_DAY_IN_MS);
            endTimeCalender.setTimeInMillis(endTimeCalender.getTimeInMillis()+CollectorTimerTask.ONE_DAY_IN_MS);
        }
        
        // start timer for periodically collect CFs which should be reset.
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new CollectorTimerTask(runningTime), beginTimeCalender.getTime(),
                CollectorTimerTask.ONE_DAY_IN_MS);
        
        if (logger.isInfoEnabled())
        {
            logger.info("CFC startup! The first collection will start at: " + beginTimeCalender.getTime()
                    + ", and will run everyday during: " + cfgBeginTime + " ~ " + cfgEndTime);
        }
    }
        
    // Set the reset operation of a CF to ClsSystem table.
    public static void setCFCResetOp(String ksName, String cfName, String udBucketName)
            throws UnavailableException, TimeoutException
    {
        String kscfName = SystemTable.kscfName(ksName, cfName);
        byte[] bKscfName = FBUtilities.utf8(kscfName);
        
        RowMutation rm = new RowMutation(ClsSystem_KS, CFC_ResetOp_KEY);
        rm.add(new QueryPath(CFC_CF, null, bKscfName), 
                FBUtilities.utf8(udBucketName), System.currentTimeMillis());
        
        // consistent write
        StorageProxy.mutateBlocking(Arrays.asList(rm), ConsistencyLevel.QUORUM);
        
        if (logger.isInfoEnabled())
        {
            logger.info("Set CFC ResetOp to ClsSystem for: " + kscfName 
                    + ", udBucketName= " + udBucketName);
        }
    }
    
    // Delete the reset operation of a CF from ClsSystem table.
    public static void deleteCFCResetOp(String ksName, String cfName, String udBucketName)
            throws IOException, UnavailableException, TimeoutException, InvalidRequestException
    {
        String kscfName = SystemTable.kscfName(ksName, cfName);
        byte[] bKscfName = FBUtilities.utf8(kscfName);
        
        // to check the matching of saved udBucketName
        IColumn savedOp = getCFCResetOp(ksName, cfName);
        if ((savedOp == null) || savedOp.isMarkedForDelete())
        {
            // no saved previous resetOp, done.
            return;
        }
        String savedUdBucketName = FBUtilities.utf8String(savedOp.value());
        if (!savedUdBucketName.equals(udBucketName))
        {
            logger.warn("Deleted CFC ResetOp ignored, since udBucketName not match: " + kscfName 
                    + ", udBucketName=" + udBucketName + ", savedUdBucketName=" + savedUdBucketName);
            throw new InvalidRequestException("Deleted CFC ResetOp ignored, since udBucketName not match: " + kscfName 
                    + ", udBucketName=" + udBucketName + ", savedUdBucketName=" + savedUdBucketName);
        }
        
        // to do delete
        RowMutation rm = new RowMutation(ClsSystem_KS, CFC_ResetOp_KEY);
        rm.delete(new QueryPath(CFC_CF, null, bKscfName), System.currentTimeMillis());
        
        // consistent write
        StorageProxy.mutateBlocking(Arrays.asList(rm), ConsistencyLevel.QUORUM);
        
        if (logger.isInfoEnabled())
        {
            logger.info("Deleted(Undo) CFC ResetOp to ClsSystem for: " + kscfName 
                    + ", udBucketName= " + udBucketName);
        }
    }
    
    // Get the reset operation of a CF from ClsSystem table.
    public static IColumn getCFCResetOp(String ksName, String cfName)
            throws IOException, UnavailableException, TimeoutException, InvalidRequestException    
    {
        String kscfName = SystemTable.kscfName(ksName, cfName);
        byte[] bKscfName = FBUtilities.utf8(kscfName);
        
        QueryPath qp = new QueryPath(CFC_CF);
        List<byte[]> columnList = new ArrayList<byte[]>();
        columnList.add(bKscfName);
        ReadCommand command = new SliceByNamesReadCommand(ClsSystem_KS, CFC_ResetOp_KEY, qp, columnList);
        List<ReadCommand> commandList = new ArrayList<ReadCommand>();
        commandList.add(command);
        
        // consistent read
        List<Row> rowList = StorageProxy.readProtocol(commandList, ConsistencyLevel.QUORUM);
        
        if ((rowList == null) || (rowList.size() == 0))
        {
            // normal! if there is no reset operation for this CF.
            return null;
        }
        else if ((rowList.size() != 1) || (rowList.get(0).cf == null))
        {
            // even the table is empty, we still got rowList.size()==1, so strange!
            // logger.warn("Read CFC all ResetOp from ClsSystem ERROR!, rowListSize=" 
            //       + rowList.size() + ", key[0]=" + rowList.get(0).key + ", CF=" + kscfName);
            return null;
        }
        
        return rowList.get(0).cf.getColumn(bKscfName);
    }
    
    // Get reset operations of all CFs from ClsSystem table.
    public static Collection<IColumn> getCFCResetOps() 
            throws IOException, UnavailableException, TimeoutException, InvalidRequestException
    {
        QueryPath qp = new QueryPath(CFC_CF);
        SliceFromReadCommand command = new SliceFromReadCommand(ClsSystem_KS, CFC_ResetOp_KEY, qp, 
                ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, false, Integer.MAX_VALUE);
        List<ReadCommand> commandList = new ArrayList<ReadCommand>();
        commandList.add(command);
        
        // consistent read
        List<Row> rowList = StorageProxy.readProtocol(commandList, ConsistencyLevel.QUORUM);
        
        if ((rowList == null) || (rowList.size() == 0))
        {
            // normal! if there is no reset operation.
            return null;
        }
        else if ((rowList.size() != 1) || (rowList.get(0).cf == null))
        {
            // even the table is empty, we still got rowList.size()==1, so strange!
            // logger.warn("Read CFC all ResetOp from ClsSystem ERROR!, rowListSize=" 
            //      + rowList.size() + ", key[0]=" + rowList.get(0).key);
            return null;
        }
        
        return rowList.get(0).cf.getSortedColumns();
    }
    
    // The config CFC time format pattern should be "HH:mm" or "HH:mm:ss".
    public static boolean checkTimeFormat(String time)
    {
        if ((time == null) || time.equals(""))
        {
            return false;
        }
          
        try
        {
            String[] hhmmss = time.split(":");
            if (hhmmss.length == 2)
            {
                int hh = Integer.parseInt(hhmmss[0]);
                int mm = Integer.parseInt(hhmmss[1]);
                if ((hh < 0) || (hh > 23) || (mm < 0) || (mm > 59))
                {
                    return false;
                }
            }
            else if (hhmmss.length == 3) 
            {
                int hh = Integer.parseInt(hhmmss[0]);
                int mm = Integer.parseInt(hhmmss[1]);
                int ss = Integer.parseInt(hhmmss[2]);
                if ((hh < 0) || (hh > 23) || (mm < 0) || (mm > 59) || (ss < 0) || (ss > 59))
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }
        catch(Exception e)
        {
            return false;
        }
        return true;
    }
    
    // Get calendar with the ms, and set time fields.
    private static Calendar getCalendar(String time, long ms)
    {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(ms);
        String[] hhmmss = time.split(":");
        
        if (hhmmss.length == 2)
        {
            int hh = Integer.parseInt(hhmmss[0]);
            int mm = Integer.parseInt(hhmmss[1]);
            calendar.set(Calendar.HOUR_OF_DAY, hh);
            calendar.set(Calendar.MINUTE, mm);
            calendar.set(Calendar.SECOND, 0);
        }
        else if (hhmmss.length == 3)
        {
            int hh = Integer.parseInt(hhmmss[0]);
            int mm = Integer.parseInt(hhmmss[1]);
            int ss = Integer.parseInt(hhmmss[2]);
            calendar.set(Calendar.HOUR_OF_DAY, hh);
            calendar.set(Calendar.MINUTE, mm);
            calendar.set(Calendar.SECOND, ss);
        }
        return calendar;
    }
    
}
