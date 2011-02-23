package spiralcraft.sql.data.store;

import java.sql.Connection;

import spiralcraft.sql.jdbc.ConnectionWrapper;

public class SqlStoreConnection
  extends ConnectionWrapper
{

  
  public SqlStoreConnection(Connection connection)
  { this.connection=connection;
  }
  
  

  
}
