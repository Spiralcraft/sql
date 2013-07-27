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


import spiralcraft.data.access.SerialCursor;

import spiralcraft.data.FieldSet;
import spiralcraft.data.DataException;
import spiralcraft.data.Tuple;

import spiralcraft.sql.data.ResultSetMapping;
import spiralcraft.sql.data.SerialResultSetCursor;
import spiralcraft.time.Clock;


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
  
  private ResultSetMapping resultSetMapping;
  
  public BoundQueryStatement(SqlStore store,FieldSet resultFields)
  { super(store,resultFields);
  }
  

  /**
   * Specify how the nested Tuple structure of the result maps to the flat resultSet
   */
  public void setResultSetMapping(ResultSetMapping mapping)
  { this.resultSetMapping=mapping;
  }

  
  /**
   * Execute the Query by allocating a PreparedStatement from the SqlStore,
   *   applying parameters, and delivering the result via a SerialCursor.
   */
  public SerialCursor<Tuple> execute()
    throws DataException
  {
    // log.fine("BoundQueryStatement: Preparing "+statementText);
    try
    {
    
      Connection connection=store.getContextConnection();
    
      PreparedStatement statement=connection.prepareStatement(statementText);
      Object[] parameters=makeParameters();
      long time=0;
      if (logLevel.isFine())
      { 
        log.fine(toString()+": Executing "+statementText+"\r\n"
          +formatParameters(parameters));
        time=Clock.instance().timeNanos();
      }
      applyParameters(statement,parameters);
      ResultSet rs=statement.executeQuery();
      SerialResultSetCursor result=null;
      if (dataFields!=null)
      { result=new SerialResultSetCursor(dataFields,rs,resultSetMapping);
      }
      else
      { result=new SerialResultSetCursor(rs);
      }
      result.setConnection(connection);
      result.setLogLevel(logLevel);
      result.setStatementInfo(statementText);
      if (logLevel.isFine())
      { log.fine("Duration="+ (Clock.instance().timeNanos()-time)/1000000.0+"ms");
      }
      return result;
    }
    catch (SQLException x)
    { throw new DataException("Error executing "+statementText+": "+x,x);
    }
  }
  
}
