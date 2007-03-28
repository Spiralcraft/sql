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


import spiralcraft.data.transport.SerialCursor;

import spiralcraft.data.FieldSet;
import spiralcraft.data.DataException;
import spiralcraft.sql.data.SerialResultSetCursor;


import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A Query PreparedStatement bound to a parameter context
 */
public class BoundQueryStatement
  extends BoundStatement
{
  protected FieldSet resultFields;
  protected String from;
  
  public BoundQueryStatement(SqlStore store,FieldSet resultFields)
  { 
    super(store);
    this.resultFields=resultFields;
  }
  


  
  /**
   * Execute the Query by allocating a PreparedStatement from the SqlStore,
   *   applying parameters, and delivering the result via a SerialCursor.
   */
  public SerialCursor execute()
    throws DataException
  {
    PreparedStatement statement=store.allocateStatement(statementText);
    try
    {
      applyParameters(statement);
      ResultSet rs=statement.executeQuery();
      if (resultFields!=null)
      { return new SerialResultSetCursor(resultFields,rs);
      }
      else
      { return new SerialResultSetCursor(rs);
      }
    }
    catch (SQLException x)
    { throw new DataException("Error executing "+statementText+": "+x,x);
    }
  }
  
}
