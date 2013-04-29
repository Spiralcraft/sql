package spiralcraft.sql.data.store;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

import spiralcraft.sql.jdbc.ConnectionWrapper;

public class SqlStoreConnection
  extends ConnectionWrapper
{

  private final XAConnection xa;
  
  public SqlStoreConnection(Connection connection) throws SQLException
  { 
    this.xa=null;
    this.connection=connection;
  }
  
  public SqlStoreConnection(Connection connection,XAConnection xa) throws SQLException
  { 
    this.xa=xa;
    this.connection=connection;
  }

  public XAResource getXAResource()
    throws SQLException
  { return xa!=null?xa.getXAResource():null;
  }
  
  @Override
  public void close()
    throws SQLException
  {
    super.close();
    if (xa!=null)
    { xa.close();
    }
  }
}
