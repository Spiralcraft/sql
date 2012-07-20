package spiralcraft.sql.jdbc;

import java.sql.PreparedStatement;

public class CachedPreparedStatement
  extends PreparedStatementWrapper
{

  public boolean closed;
  
  public CachedPreparedStatement(PreparedStatement statement)
  { super(statement);
  }

  @Override
  public void close()
  {
    // Don't close underlying statement
    if (!closed)
    { closed=true;
    }
  }
}
