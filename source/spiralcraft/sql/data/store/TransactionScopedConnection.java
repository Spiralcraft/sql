package spiralcraft.sql.data.store;

import java.sql.Connection;

import spiralcraft.log.ClassLog;
import spiralcraft.log.Level;
import spiralcraft.sql.jdbc.ConnectionWrapper;

public class TransactionScopedConnection
  extends ConnectionWrapper
  implements Connection
{
  private static final ClassLog log
    =ClassLog.getInstance(TransactionScopedConnection.class);
  private static final Level logLevel
    =ClassLog.getInitialDebugLevel(TransactionScopedConnection.class,Level.INFO);

  
  public TransactionScopedConnection(Connection connection)
  { this.connection=connection;
  }
  
  @Override
  public void setAutoCommit(boolean autoCommit)
  { 
    if (logLevel.isDebug())
    { log.fine("Ignoring setAutoCommit("+autoCommit+") in transaction");
    }
  }
  
  @Override
  public void close()
  { 
    if (logLevel.isDebug())
    { log.fine("Ignoring close() in transaction");
    }
  }
  
}
