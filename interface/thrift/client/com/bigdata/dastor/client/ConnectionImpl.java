package com.bigdata.dastor.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.bigdata.dastor.thrift.Column;
import com.bigdata.dastor.thrift.ColumnOrSuperColumn;
import com.bigdata.dastor.thrift.ColumnParent;
import com.bigdata.dastor.thrift.ColumnPath;
import com.bigdata.dastor.thrift.ConsistencyLevel;
import com.bigdata.dastor.thrift.Dastor;
import com.bigdata.dastor.thrift.InvalidRequestException;
import com.bigdata.dastor.thrift.NotFoundException;
import com.bigdata.dastor.thrift.SlicePredicate;
import com.bigdata.dastor.thrift.SliceRange;
import com.bigdata.dastor.thrift.TimedOutException;
import com.bigdata.dastor.thrift.UnavailableException;

public class ConnectionImpl implements Connection
{    
    private boolean closed = false;
    private TTransport transport = null;
    private Dastor.Client dastor = null;
        
    public ConnectionImpl(String host, int port)
    throws DastorTransportException
    {
        this(host, port, 0);
    }
    
    public ConnectionImpl(String host, int port, int timeout)
    throws DastorTransportException
    {
        connect(host, port, timeout);
    }
    
    private void connect(String host, int port, int timeout)
    throws DastorTransportException
    {
        TSocket socket = new TSocket(host, port, timeout);
        transport = socket;
        TBinaryProtocol binaryProtocol = new TBinaryProtocol(transport);
        dastor = new Dastor.Client(binaryProtocol);
        
        try
        {
            transport.open();
        }
        catch (TTransportException e)
        {
            throw new DastorTransportException("Failed to connect " + host + ":" + port);
        }
    }
    
    private ConsistencyLevel getConsistencyLevel(GuaranteeFactor gFactor)
    {
        switch (gFactor)
        {
            case NONE:
                return ConsistencyLevel.ZERO;
            case ANY:
                return ConsistencyLevel.ANY;
            case ONE:
                return ConsistencyLevel.ONE;
            case QUORUM:
                return ConsistencyLevel.QUORUM;
            case ALL:
                return ConsistencyLevel.ALL;
            default:
                return ConsistencyLevel.QUORUM;
        }
    }

    @Override
    public boolean isClosed()
    {
        return closed;
    }

    @Override
    public void close()
    {
        if (transport != null)
        {
            transport.close();
            transport = null;
        }
        closed = true;
    }
    
    @Override
    public void put(String space, String bucket, String key, Cell cell, GuaranteeFactor gFactor)
    throws DastorException
    {
        assert (dastor != null);
        if (closed)
        {
            throw new DastorTransportException("Connection is closed");
        }
        
        ColumnPath columnPath = new ColumnPath(bucket);
        columnPath.setColumn(cell.name);
        
        try
        {
            dastor.insert(space, key, columnPath, cell.value, cell.timestamp, getConsistencyLevel(gFactor)); 
        }
        catch (InvalidRequestException e1)
        {
            throw new DastorInvalidRequestException();
        }
        catch (UnavailableException e2)
        {
            throw new DastorUnavailableException();
        }
        catch (TimedOutException e2)
        {
            throw new DastorTimeOutException();
        }
        catch (TException e3)
        {
            throw new DastorException();
        }
    }
    
    @Override
    public void put(String space, String bucket, String key, List<Cell> cellList, GuaranteeFactor gFactor)
    throws DastorException
    {
        assert (dastor != null);
        if (closed)
        {
            throw new DastorTransportException("Connection is closed");
        }
        
        List<ColumnOrSuperColumn> columnList = new ArrayList<ColumnOrSuperColumn>(cellList.size());
        for (Cell cell : cellList)
        {
            Column col = new Column(cell.name, cell.value, cell.timestamp);
            ColumnOrSuperColumn column = new ColumnOrSuperColumn();
            column.setColumn(col);
            columnList.add(column);
        }
        
        Map<String, List<ColumnOrSuperColumn>> bktmap = new HashMap<String, List<ColumnOrSuperColumn>>(1);
        bktmap.put(bucket, columnList);

        try
        {
            dastor.batch_insert(space, key, bktmap, getConsistencyLevel(gFactor));
        }
        catch (InvalidRequestException e1)
        {
            throw new DastorInvalidRequestException();
        }
        catch (UnavailableException e2)
        {
            throw new DastorUnavailableException();
        }
        catch (TimedOutException e2)
        {
            throw new DastorTimeOutException();
        }
        catch (TException e3)
        {
            throw new DastorException();
        }
    }
    
    @Override
    public Cell get(String space, String bucket, String key, byte[] cellName, GuaranteeFactor gFactor)
    throws DastorException
    {
        ColumnPath columnPath = new ColumnPath(bucket);
        columnPath.setColumn(cellName);

        try
        {
            ColumnOrSuperColumn column = dastor.get(space, key, columnPath, getConsistencyLevel(gFactor));
            if ((column != null) && (column.getColumn() != null))
            {
                return new Cell(column.getColumn().getName(), 
                                column.getColumn().getValue(), 
                                column.getColumn().getTimestamp());
            }
            else
            {
                throw new DastorUnavailableException();
            }
        }
        catch (InvalidRequestException e1)
        {
            throw new DastorInvalidRequestException();
        }
        catch (NotFoundException e2)
        {
            throw new DastorUnavailableException();
        }
        catch (UnavailableException e3)
        {
            throw new DastorUnavailableException();
        }
        catch (TimedOutException e4)
        {
            throw new DastorTimeOutException();
        }
        catch (TException e5)
        {
            throw new DastorException();
        }
    }
    
    @Override
    public List<Cell> get(String space, String bucket, String key, List<byte[]> cellNames, GuaranteeFactor gFactor)
    throws DastorException
    {
        ColumnParent columnParent = new ColumnParent(bucket);
        SlicePredicate slicePredicate = new SlicePredicate();
        slicePredicate.setColumn_names(cellNames);
        
        try
        {
            List<ColumnOrSuperColumn> columnList = dastor.get_slice(space, key, columnParent, slicePredicate, getConsistencyLevel(gFactor));
            List<Cell> cellList = new ArrayList<Cell>(columnList.size());
            for (ColumnOrSuperColumn column : columnList)
            {
                cellList.add( new Cell(column.getColumn().getName(),
                                       column.getColumn().getValue(), 
                                       column.getColumn().getTimestamp()) );
            }
            return cellList;
        }
        catch (InvalidRequestException e1)
        {
            throw new DastorInvalidRequestException();
        }
        catch (UnavailableException e3)
        {
            throw new DastorUnavailableException();
        }
        catch (TimedOutException e4)
        {
            throw new DastorTimeOutException();
        }
        catch (TException e5)
        {
            throw new DastorException();
        }
    }
    
    @Override
    public List<Cell> get(String space, String bucket, String key, CellRange range, boolean desc, GuaranteeFactor gFactor)
    throws DastorException
    {
        if (range.startCellName == null)
        {
            range.startCellName = EMPTY_BYTE_ARRAY;
        }
        if (range.endCellName == null)
        {
            range.endCellName = EMPTY_BYTE_ARRAY;
        }
        ColumnParent columnParent = new ColumnParent(bucket);
        SliceRange sliceRange = new SliceRange(range.startCellName, range.endCellName, desc, range.limitCount);
        SlicePredicate slicePredicate = new SlicePredicate();
        slicePredicate.setSlice_range(sliceRange);
        
        try
        {
            List<ColumnOrSuperColumn> columnList = dastor.get_slice(space, key, columnParent, slicePredicate, getConsistencyLevel(gFactor));
            List<Cell> cellList = new ArrayList<Cell>(columnList.size());
            for (ColumnOrSuperColumn column : columnList)
            {
                cellList.add( new Cell(column.getColumn().getName(),
                                       column.getColumn().getValue(), 
                                       column.getColumn().getTimestamp()) );
            }
            return cellList;
        }
        catch (InvalidRequestException e1)
        {
            throw new DastorInvalidRequestException();
        }
        catch (UnavailableException e3)
        {
            throw new DastorUnavailableException();
        }
        catch (TimedOutException e4)
        {
            throw new DastorTimeOutException();
        }
        catch (TException e5)
        {
            throw new DastorException();
        }
    }
    
    @Override
    public List<Cell> get(String space, String bucket, String key, boolean desc, GuaranteeFactor gFactor)
    throws DastorException
    {
        ColumnParent columnParent = new ColumnParent(bucket);
        SliceRange sliceRange = new SliceRange(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY, desc, Integer.MAX_VALUE);
        SlicePredicate slicePredicate = new SlicePredicate();
        slicePredicate.setSlice_range(sliceRange);
        
        try
        {
            List<ColumnOrSuperColumn> columnList = dastor.get_slice(space, key, columnParent, slicePredicate,  getConsistencyLevel(gFactor));
            List<Cell> cellList = new ArrayList<Cell>(columnList.size());
            for (ColumnOrSuperColumn column : columnList)
            {
                cellList.add( new Cell(column.getColumn().getName(),
                                       column.getColumn().getValue(), 
                                       column.getColumn().getTimestamp()) );
            }
            return cellList;
        }
        catch (InvalidRequestException e1)
        {
            throw new DastorInvalidRequestException();
        }
        catch (UnavailableException e3)
        {
            throw new DastorUnavailableException();
        }
        catch (TimedOutException e4)
        {
            throw new DastorTimeOutException();
        }
        catch (TException e5)
        {
            throw new DastorException();
        }
    }
}
