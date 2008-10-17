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

import java.sql.Connection;
import java.sql.SQLException;

public class SqlResourceManager
  extends ResourceManager<SqlBranch>
{

  private SqlStore sqlStore;
  
  public SqlResourceManager(SqlStore sqlStore)
  { this.sqlStore=sqlStore;
  }
  
  public void deallocateConnection(Connection connection)
  {
    try
    { 
      // XXX Adapt for pool
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
      // XXX Adapt for pool
      Connection connection=sqlStore.getDataSource().getConnection();
      connection.setAutoCommit(false);
      return connection;
    }
    catch (SQLException x)
    { throw new DataException("Error allocating connection: "+x,x);
    }
    
    
  }
  
  @Override
  public SqlBranch createBranch(Transaction transaction)
    throws TransactionException
  { return new SqlBranch(this);
  }

}
