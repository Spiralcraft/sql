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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Wrapper;

import spiralcraft.json.ArrayNode;
import spiralcraft.json.Node;
import spiralcraft.json.NullNode;
import spiralcraft.json.NumberNode;
import spiralcraft.json.ObjectNode;
import spiralcraft.json.StringNode;

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
  
  public static String queryToJSON(Connection conn,String sql)
    throws SQLException
  {
    Statement st=null;
    ObjectNode json = new ObjectNode(null);
    try
    {
      st=conn.createStatement();
      boolean res = st.execute(sql);
      if (!res)
      { json.addChild(new NumberNode("updateCount",st.getUpdateCount()));
      }
      else
      {
        ArrayNode rsArray = new ArrayNode("results");
        json.addChild(rsArray);
        do
        {
          ArrayNode data = new ArrayNode(null);
          rsArray.addChild(data);
          
          ResultSet rs=st.getResultSet();
          ResultSetMetaData rsmd = rs.getMetaData();
          int numColumns = rsmd.getColumnCount();
          
          while (rs.next())
          { 
            ObjectNode row=new ObjectNode(null);
            data.addChild(row);
            for (int i=1; i<numColumns; i++)
            {
              String colname=rsmd.getColumnName(i);
              Object value=rs.getObject(colname);
              Node cell;
              if (value==null)
              { cell=new NullNode(colname);
              }
              else
              { cell=new StringNode(colname,value.toString());
              }
              row.addChild(cell);
            }
          }
          rs.close();
        }
        while (st.getMoreResults());
      }
    }
    catch (Exception x)
    { json.addChild(new StringNode("Exception",x.toString()));
    }
    finally
    { 
      if (st!=null)
      { st.close();
      }
    }
    return Node.formatToString(json);
  }

}
