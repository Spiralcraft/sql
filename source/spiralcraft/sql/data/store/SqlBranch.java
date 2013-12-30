//
// Copyright (c) 1998,2007 Michael Toth
// Spiralcraft Inc., All Rights Reserved
//
// This package is part of the Spiralcraft project and is licensed under
// a multiple-license framework.
//
// You may not use this file except in compliance with the terms found in the
// SPIRALCRAFT-LICENSE.txt file at the top of this distribution, or available
// at http://www.spiralcraft.org/licensing/SPIRALCRAFT-LICENSE.txt.
//
// Unless otherwise agreed to in writing, this software is distributed on an
// "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.
//
package spiralcraft.sql.data.store;

import spiralcraft.data.DataException;

import spiralcraft.data.transaction.Branch;
import spiralcraft.data.transaction.Transaction;
import spiralcraft.data.transaction.Transaction.State;
import spiralcraft.data.transaction.TransactionException;
import spiralcraft.log.ClassLog;
import spiralcraft.log.Level;
import spiralcraft.sql.util.SQLUtil;

import java.sql.Connection;
import java.sql.SQLException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class SqlBranch
  implements Branch
{

  private static final ClassLog log
    =ClassLog.getInstance(SqlBranch.class);
  private static Level logLevel
    =ClassLog.getInitialDebugLevel(SqlBranch.class,Level.INFO);
  
  private Connection connection;
  private Connection userConnection;
  private State state;
  private SqlResourceManager resourceManager;
  private final XAResource xar;
  private final Xid xid;
  
  
  public SqlBranch(SqlResourceManager resourceManager,Transaction tx)
    throws TransactionException
  {
    state=State.STARTED;
    xid=tx.nextXid();
    
    this.resourceManager=resourceManager;
    try
    { 
      
      connection=resourceManager.allocateConnection();
      userConnection=new TransactionScopedConnection(connection);
      if (logLevel.isFine())
      { log.fine("Connection: "+connection);
      }
      try
      { 
        SqlStoreConnection ssc
          =SQLUtil.tryUnwrap
            (connection,SqlStoreConnection.class);
        if (ssc!=null)
        {
          xar=ssc.getXAResource();
          if (xar!=null)
          { xar.start(xid,XAResource.TMNOFLAGS);
          }
        }
        else
        { xar=null;
        }
      }
      catch (SQLException x)
      { 
        throw new TransactionException
          ("Error associating connection with transaction",x);
      }
      catch (XAException e)
      {
        throw new TransactionException
          ("Error associating connection with transaction",e);
      }
    }
    catch (DataException x)
    { throw new TransactionException("Error allocating connection: "+x,x);
    }
  }
  
  public Connection getConnection()
  { return userConnection;
  }
  
  @Override
  public void commit()
    throws TransactionException
  {
    try
    { 
      
      if (xar!=null)
      {
        try
        { 
          if (logLevel.isFine())
          { log.fine("Calling xar.commit("+xid+")");
          }
          xar.commit(xid,false);
        }
        catch (XAException e)
        { throw new TransactionException("Error preparing XA resource",e);
        }
        
      }
      else
      { 
        // Only commit the connection if no global transaction
        connection.commit();
      }
      if (logLevel.isFine())
      { log.fine("Committed");
      }
    }
    catch (SQLException x)
    { throw new TransactionException("Commit failed: "+x,x);
    } 
    state=State.COMMITTED;
  }

  @Override
  public State getState()
  { return state;
  }

  @Override
  public void prepare()
    throws TransactionException
  { 
    if (xar!=null)
    { 
      try
      { 
        xar.end(xid,XAResource.TMSUCCESS);
        if (logLevel.isFine())
        { log.fine("Calling xar.prepare("+xid+")");
        }
        xar.prepare(xid);
      }
      catch (XAException e)
      { throw new TransactionException("Error preparing XA resource",e);
      }
    }
    state=State.PREPARED;    
  }

  @Override
  public void rollback()
    throws TransactionException
  {
    
    try
    { 
      if (xar!=null)
      { 
        
        try
        { xar.end(xid,XAResource.TMFAIL); 
        }
        catch (XAException e)
        { log.log(Level.WARNING,"Error ending transaction "+xid,e);
        }
        
        if (logLevel.isFine())
        { log.fine("Calling xar.rollback("+xid+")");
        }
        
        try
        { xar.rollback(xid);
        }
        catch (XAException e)
        { log.log(Level.WARNING,"Error rolling back transaction "+xid,e);
        }

      }
      else
      { connection.rollback();
      }
      if (logLevel.isFine())
      { log.fine("Rollback");
      }
    }
    catch (SQLException x)
    { throw new TransactionException("Rollback failed: "+x,x);
    } 
    state=State.ABORTED;
  }
  
  @Override
  public void complete() 
    throws TransactionException 
  { 
    if (logLevel.isFine())
    { log.fine("Completing in state "+state.name());
    }
   
    if (connection!=null)
    { resourceManager.deallocateConnection(connection);
    }
  }
  
  @Override
  public boolean is2PC()
  { return xar!=null;
  }

  @Override
  public String toString()
  { return super.toString()+": "+resourceManager.toString();
  }
}
