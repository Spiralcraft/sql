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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Wrapper;

public class SQLUtil
{
  public static String dumpResultSet(ResultSet rs)
    throws SQLException
  {
    ResultSetMetaData md=rs.getMetaData();
    StringBuffer buf=new StringBuffer();

    int count=md.getColumnCount();
    buf.append("\r\n");
    for (int i=0;i<count;i++)
    { 
      if (i>0)
      { buf.append(",");
      }
      buf.append(md.getColumnName(i+1));
    }
    
    while (rs.next())
    {
      buf.append("\r\n");
      for (int i=0;i<count;i++)
      { 
        if (i>0)
        { buf.append(",");
        }
        buf.append(rs.getObject(i+1));
      }
    }
    buf.append("\r\n");
    return buf.toString();
  }
  
  public static <T> T tryUnwrap(Wrapper wrapper,Class<T> iface)
  {
    try
    { 
      if (!wrapper.isWrapperFor(iface))
      { return null;
      }
      return wrapper.unwrap(iface);
    }
    catch (SQLException x)
    { return null;
    }
  }
}
