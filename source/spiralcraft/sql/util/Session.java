//
// Copyright (c) 1998,2009 Michael Toth
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
package spiralcraft.sql.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import spiralcraft.data.DataException;
import spiralcraft.log.ClassLog;
import spiralcraft.util.ArrayUtil;

/**
 * A context for performing a common actions on an open
 *   database connection.
 *   
 * @author mike
 *
 */
public class Session
{

  protected final ClassLog log=ClassLog.getInstance(getClass());
  
  
  protected Connection connection;
  
  public void start(Connection connection)
  { this.connection=connection;
  }
  
  
  
  public void commit()
    throws SQLException
  { 
    log.debug("Committing");
    this.connection.commit();
  }
  
  public void stop()
  { this.connection=null;
  }
  
  public ResultSet executeQuery(String sql)
    throws SQLException,DataException
  {
    
    Statement st=null;
    ResultSet rs=null;

    st=connection.createStatement(); 
    log.debug("Executing: "+sql);
    rs=st.executeQuery(sql);
    return rs;
  }
  
  public int executeUpdate(String sql)
    throws SQLException
  {
    Statement st=null;
    try
    {
      st=connection.createStatement(); 
      log.debug("Executing: "+sql);
      return st.executeUpdate(sql);
    }
    finally
    { st.close();
    }
  }

  public int executePreparedUpdate(String sql,Object ... params)
    throws SQLException
  {
    PreparedStatement st=null;
    try
    {
      st=connection.prepareStatement(sql); 
      for (int i=0;i<params.length;i++)
      { st.setObject(i+1, params[i]);
      }
      log.debug("Executing: "+sql+" {"+ArrayUtil.format(params,",","'")+"}");
      return st.executeUpdate();
    }
    finally
    { st.close();
    }
  }  
  
  public ResultSet executePreparedQuery(String sql,Object ... params)
    throws SQLException
  {
    PreparedStatement st=null;

    st=connection.prepareStatement(sql); 
    for (int i=0;i<params.length;i++)
    { st.setObject(i+1, params[i]);
    }
    log.debug("Executing: "+sql+" {"+ArrayUtil.format(params,",","'")+"}");
    return st.executeQuery();
  }  
}
