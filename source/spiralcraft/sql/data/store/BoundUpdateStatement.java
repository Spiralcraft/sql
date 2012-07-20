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



import spiralcraft.data.FieldSet;
import spiralcraft.data.DataException;


import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Connection;

/**
 * A Query PreparedStatement bound to a parameter context
 */
public class BoundUpdateStatement
  extends BoundStatement
{
  protected String from;
  
  public BoundUpdateStatement(SqlStore store,FieldSet dataFields)
  { super(store,dataFields);
  }
  
  /**
   * Execute the Query by allocating a PreparedStatement from the SqlStore,
   *   applying parameters, and returning the number of rows updated.
   */
  public int execute()
    throws DataException
  {
    Connection connection=store.getContextConnection();
    
    try
    {
      PreparedStatement statement=connection.prepareStatement(statementText);
      applyParameters(statement);
      int results=statement.executeUpdate();
      // log.fine("Closing "+statement);
      statement.close();
      return results;
    }
    catch (SQLException x)
    { throw new DataException("Error executing "+statementText+": "+x,x);
    }
    finally
    { 
      try
      { connection.close();
      }
      catch (SQLException x)
      { throw new DataException("Error closing connection: "+x,x);
      }
    }
  }
  
}
