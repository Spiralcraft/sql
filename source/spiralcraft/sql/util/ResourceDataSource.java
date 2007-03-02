//
// Copyright (c) 1998,2005 Michael Toth
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

import spiralcraft.data.persist.XmlBean;
import spiralcraft.data.persist.PersistenceException;

import java.net.URI;

import java.io.PrintWriter;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public class ResourceDataSource
  implements DataSource
{

  private final DataSource delegate;
  
  public ResourceDataSource(URI resourceUri)
    throws PersistenceException
  {
    XmlBean<DataSource> ref
      =new XmlBean<DataSource>(null,resourceUri);
    delegate=ref.get();
  }
  
  public Connection getConnection()
    throws SQLException
  { return delegate.getConnection();
  }

  public Connection getConnection(String username, String password)
    throws SQLException
  { return delegate.getConnection(username,password);
  }

  public PrintWriter getLogWriter()
    throws SQLException
  { return delegate.getLogWriter();
  }

  public int getLoginTimeout()
    throws SQLException
  { return delegate.getLoginTimeout();
  }

  public void setLogWriter(PrintWriter out) throws SQLException
  { delegate.setLogWriter(out);
  }

  public void setLoginTimeout(int seconds) throws SQLException
  { delegate.setLoginTimeout(seconds);
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException
  { return delegate.isWrapperFor(iface);
  }

  public <T> T unwrap(Class<T> iface) throws SQLException
  { return delegate.unwrap(iface);
  }
  

}
