package spiralcraft.sql;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for tools which use database connections to obtain and
 *   release Connections.
 */
public interface ConnectionManager
{
  /**
   * Obtain a database connection
   */
  public Connection openConnection()
    throws SQLException;
  
  /**
   * Close a database connection
   */
  public void closeConnection(Connection connection)
    throws SQLException;
  
}