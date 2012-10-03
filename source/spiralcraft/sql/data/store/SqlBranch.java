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
import spiralcraft.data.transaction.Transaction.State;
import spiralcraft.data.transaction.TransactionException;
import spiralcraft.log.ClassLog;
import spiralcraft.log.Level;

import java.sql.Connection;
import java.sql.SQLException;

public class SqlBranch
  implements Branch
{

  private static final ClassLog log
    =ClassLog.getInstance(SqlBranch.class);
  private static Level logLevel
    =ClassLog.getInitialDebugLevel(SqlBranch.class,Level.INFO);
  
  private Connection connection;
  private State state;
  private SqlResourceManager resourceManager;
  
  
  public SqlBranch(SqlResourceManager resourceManager)
    throws TransactionException
  {
    state=State.STARTED;
    this.resourceManager=resourceManager;
    try
    { 
      
      connection=resourceManager.allocateConnection();
    }
    catch (DataException x)
    { throw new TransactionException("Error allocating connection: "+x,x);
    }
  }
  
  public Connection getConnection()
  { 
    // XXX Return a fascade that won't close the real connection on close()
    return connection;
  }
  
  @Override
  public void commit()
    throws TransactionException
  {
    try
    { 
      connection.commit();
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
  { state=State.PREPARED;    
  }

  @Override
  public void rollback()
    throws TransactionException
  {
    
    try
    { 
      connection.rollback();
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
  { return false;
  }

}
