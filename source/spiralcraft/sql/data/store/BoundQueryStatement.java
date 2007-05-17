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

import spiralcraft.util.tree.LinkedTree;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A Query PreparedStatement bound to a parameter context
 */
public class BoundQueryStatement
  extends BoundStatement
{
  
  private LinkedTree<Integer> foldTree;
  
  public BoundQueryStatement(SqlStore store,FieldSet resultFields)
  { super(store,resultFields);
  }
  

  /**
   * Specify how the nested Tuple structure of the result maps to the flat resultSet
   */
  public void setFoldTree(LinkedTree<Integer> foldTree)
  { this.foldTree=foldTree;
  }

  
  /**
   * Execute the Query by allocating a PreparedStatement from the SqlStore,
   *   applying parameters, and delivering the result via a SerialCursor.
   */
  public SerialCursor execute()
    throws DataException
  {
    // XXX: Presents problems re. returning with an open result set and connection.
    // XXX: This might want to take a DataConsumer instead for call-back.
    
    System.err.println("BoundQueryStatement: Preparing "+statementText);
    
    Connection connection=store.allocateConnection();
    try
    {
      PreparedStatement statement=connection.prepareStatement(statementText);
      applyParameters(statement);
      ResultSet rs=statement.executeQuery();
      if (dataFields!=null)
      { return new SerialResultSetCursor(dataFields,rs,foldTree);
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
