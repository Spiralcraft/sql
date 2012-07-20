//
// Copyright (c) 2009 Michael Toth
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
package spiralcraft.sql.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.HashMap;

/**
 * Caches results of the prepareStatement(String sql) method for the life
 *   of the connection.
 */
public class StatementCachingConnection
  extends ConnectionWrapper
{

  protected HashMap<String,PreparedStatement> preparedStatementCache
    =new HashMap<String,PreparedStatement>();
  
  public StatementCachingConnection(Connection connection)
  { this.connection=connection;
  }
  
  
  @Override
  public synchronized PreparedStatement prepareStatement(String sql)
    throws SQLException
  {
    
    PreparedStatement statement=preparedStatementCache.get(sql);
    if (statement==null)
    { 
      statement=super.prepareStatement(sql);
      preparedStatementCache.put(sql,statement);
    }
    else
    { 
      // TODO: Deal with making sure statement is "reset"
    }
    return new CachedPreparedStatement(statement);
  }
}
