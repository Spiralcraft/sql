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

import java.sql.Connection;
import java.sql.SQLException;

public class SqlBranch
  implements Branch
{

  private Connection connection;
  private State state;
  private SqlResourceManager resourceManager;
  
  public SqlBranch(SqlResourceManager resourceManager)
    throws TransactionException
  {
    state=State.STARTED;
    this.resourceManager=resourceManager;
    try
    { connection=resourceManager.allocateConnection();
    }
    catch (DataException x)
    { throw new TransactionException("Error allocating connection: "+x,x);
    }
  }
  
  public Connection getConnection()
  { return connection;
  }
  
  public void commit()
    throws TransactionException
  {
    try
    { connection.commit();
    }
    catch (SQLException x)
    { throw new TransactionException("Commit failed: "+x,x);
    } 
    state=State.COMMITTED;
  }

  public State getState()
  { return state;
  }

  public void prepare()
  { state=State.PREPARED;    
  }

  public void rollback()
    throws TransactionException
  {
    
    try
    { connection.rollback();
    }
    catch (SQLException x)
    { throw new TransactionException("Rollback failed: "+x,x);
    } 
    state=State.ABORTED;
  }
  
  public void complete()
  { 
    if (connection!=null)
    { resourceManager.deallocateConnection(connection);
    }
  }
  
  public boolean is2PC()
  { return false;
  }

}
