package com.bigdata.dastor.cfc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TimerTask;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.bigdata.dastor.db.ColumnFamilyStore;
import com.bigdata.dastor.db.IColumn;
import com.bigdata.dastor.db.SystemTable;
import com.bigdata.dastor.db.Table;
import com.bigdata.dastor.service.StorageService;
import com.bigdata.dastor.utils.FBUtilities;

public class CollectorTimerTask extends TimerTask
{
    private static final Logger logger = Logger.getLogger(CollectorTimerTask.class);

    // milliseconds of one day
    public static final long ONE_DAY_IN_MS = (24*3600*1000);

    // 60s (1 minute) one time & 120 times stands for 2 hours
    private static final long POLL_DELTA = (60*1000);
    private static final long POLL_TIMES = ((2*3600*1000)/POLL_DELTA);
    
    private final long runingTime;
    
    public CollectorTimerTask(long runingTime)
    {
        this.runingTime = runingTime;
    }
    
    // Get all CFs which should be reset.
    private List<IColumn> getAllShouldResetCFs()
    {
        Collection<IColumn> resetOps = null;
        try
        {
            resetOps = Collector.getCFCResetOps();
        }
        catch (Exception e1)
        {
            logger.warn("CFC getCFCResetOps Exception " 
                    + e1 + "\n" + ExceptionUtils.getFullStackTrace(e1));
            return null;
        }
        
        if (resetOps == null)
        {
            // no reset operation.
            return null;
        }
        
        ArrayList<IColumn> shouldResetList = new ArrayList<IColumn>();        
        for (IColumn c : resetOps)
        {
            byte[] bOpKscf = c.name();
            String opKscf = FBUtilities.utf8String(bOpKscf);
            
            if (c.isMarkedForDelete())
            {
                // this ResetOp of CF is already undo by user, skip.
                if (logger.isInfoEnabled())
                {
                    logger.info("ResetOp of the CF isMarkedForDelete, skip: " + opKscf);
                }
                continue;
            }

            // get ColumnFamilyStore
            Table tab;
            try
            {
                tab = Table.open(SystemTable.ksOfKscfName(opKscf));
            }
            catch (IOException e2)
            {
                continue;
            }
            if (tab == null)
                continue;
            ColumnFamilyStore cfs = tab.getColumnFamilyStore(SystemTable.cfOfKscfName(opKscf));
            if (cfs == null)
                continue;
            
            // here we need not get CF status from local SystemTable, since its same as ColumnFamilyStore.
                    
            if (cfs.getStatus() == ColumnFamilyStore.CF_STATUS_RESETING)
            {
                // it's already in reseting, skip.
                if (logger.isInfoEnabled())
                {
                    logger.info("The ResetOp of the CF is already in reseting, skip: " + opKscf);
                }
                continue;
            }

            if (cfs.getStatusTimestamp() > c.timestamp())
            {
                // it's a old reset operation request, skip.
                if (logger.isInfoEnabled())
                {
                    logger.info("The ResetOp of the CF is out-of-date, skip: " + opKscf);
                }
                continue;
            }
                        
            shouldResetList.add(c);
        }
        return shouldResetList;
    }
    
    private void waitForReseting(ColumnFamilyStore cfs, String kscf)
    {
        long checkTimes = 0;
        do
        {
            try
            {
                Thread.sleep(POLL_DELTA);
            }
            catch (InterruptedException e) 
            {
                logger.error("InterruptedException when sleep");
            }
            
            checkTimes++;
            if (checkTimes >= POLL_TIMES)
            {
                logger.warn("Too long for waiting for reseting the CF: " + kscf);
                break;
            }
        } while (cfs.getStatus() == ColumnFamilyStore.CF_STATUS_RESETING);
        
        if (cfs.getStatus() != ColumnFamilyStore.CF_STATUS_RESETING)
        {
            logger.info("Reset done of CF: " + kscf);
        }
    }
    
    @Override
    public void run() {
        logger.info("CFC period task start!");
        
        long finishTime = System.currentTimeMillis() + runingTime;

        List<IColumn> toResetList = getAllShouldResetCFs();
        if ((toResetList == null) || (toResetList.size() == 0))
        {
            logger.info("CFC period task stop, no CF should be reset.");
            return;
        }
        
        for (IColumn c : toResetList)
        {
            if (System.currentTimeMillis() >= finishTime)
            {
                // it's time to finish, the to-be-reset CFs will be reset in next period.
                logger.info("CFC period task break dur to finish time!");
                break;
            }
            
            byte[] bOpKscf = c.name();
            String opKscf = FBUtilities.utf8String(bOpKscf);
            String ksName = SystemTable.ksOfKscfName(opKscf);
            String cfName = SystemTable.cfOfKscfName(opKscf);
            
            // get ColumnFamilyStore
            Table tab;
            try
            {
                tab = Table.open(ksName);
            }
            catch (IOException e1)
            {
                continue;
            }
            if (tab == null)
                continue;
            ColumnFamilyStore cfs = tab.getColumnFamilyStore(cfName);
            if (cfs == null)
                continue;
                        
            // start the reset process.
            StorageService.instance.forceResetColumnFamily(ksName, cfName);

            // wait one CF-reseting finish and continue the next.
            waitForReseting(cfs, opKscf);
        }
        
        logger.info("CFC period task stop.");
    }

}
