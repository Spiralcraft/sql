package spiralcraft.sql.data.store;

import java.sql.Connection;
import java.sql.SQLException;

import spiralcraft.sql.jdbc.ConnectionWrapper;

public class SqlStoreConnection
  extends ConnectionWrapper
{

  
  public SqlStoreConnection(Connection connection) throws SQLException
  { this.connection=connection;
  }
  
  

  
}
