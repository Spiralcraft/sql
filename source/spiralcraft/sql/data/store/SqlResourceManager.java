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

import spiralcraft.data.transaction.ResourceManager;
import spiralcraft.data.transaction.Transaction;
import spiralcraft.data.transaction.TransactionException;
import spiralcraft.log.ClassLog;
import spiralcraft.log.Level;

import java.sql.Connection;
import java.sql.SQLException;

public class SqlResourceManager
  extends ResourceManager<SqlBranch>
{

  private static final ClassLog log
    =ClassLog.getInstance(SqlResourceManager.class);
  private Level logLevel
    =ClassLog.getInitialDebugLevel(SqlResourceManager.class,Level.INFO);
  
  private SqlStore sqlStore;
  
  public SqlResourceManager(SqlStore sqlStore)
  { this.sqlStore=sqlStore;
  }
  
  public void deallocateConnection(Connection connection)
  {
    try
    { 
      if (logLevel.isFine())
      { log.fine("Deallocating "+connection);
      }
      connection.close();
    }
    catch (SQLException x)
    { x.printStackTrace();
    }
  }
  
  public Connection allocateConnection()
    throws DataException
  { 
    try
    {
      Connection connection=sqlStore.checkoutConnection();
      connection.setAutoCommit(false);
      if (logLevel.isFine())
      { log.fine("Allocated "+connection);
      }
      return connection;
    }
    catch (SQLException x)
    { throw new DataException("Error allocating connection: "+x,x);
    }
    
    
  }
  
  @Override
  public SqlBranch createBranch(Transaction transaction)
    throws TransactionException
  { return new SqlBranch(this,transaction);
  }

  @Override
  public String toString()
  { return super.toString()+": store="+sqlStore.getName();
  }
}
