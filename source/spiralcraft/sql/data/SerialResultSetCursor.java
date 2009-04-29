//
// Copyright (c) 1998,2007 Michael Toth
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
package spiralcraft.sql.data;


import spiralcraft.data.DataException;
import spiralcraft.data.FieldSet;
import spiralcraft.data.Identifier;
import spiralcraft.data.Tuple;
import spiralcraft.data.Type;
import spiralcraft.data.access.SerialCursor;

import spiralcraft.data.lang.CursorBinding;
import spiralcraft.data.spi.ArrayTuple;
import spiralcraft.lang.BindException;
import spiralcraft.lang.Channel;


import spiralcraft.util.tree.LinkedTree;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SerialResultSetCursor
  implements SerialCursor<Tuple>
{
  protected final FieldSet fieldSet;
  protected final ResultSetTuple tuple;
  protected ResultSet resultSet;
  protected boolean autoClose=true;
  protected boolean noCopy=false;
  protected Identifier relationId=null;
  private static CursorBinding<Tuple,?> binding;
  protected Statement statement;
  protected Connection connection;
  
  
  /**
   * 
   * @param resultSet The JDBC ResultSet to scan
   * @param noCopy Whether a single Tuple should be reused as a buffer. If
   *   true, data in the Tuple will change as the cursor is advanced and
   *   limited garbage will be created. If false, data will be copied into
   *   an immutable Tuple every time the cursor is advanced.
   * @throws DataException
   */
  @SuppressWarnings("unchecked") // Not genericized
  public SerialResultSetCursor(ResultSet resultSet)
    throws DataException
  { 
    this.resultSet=resultSet;
    try
    { 
      ResultSetScheme fieldSet=new ResultSetScheme(resultSet.getMetaData());
      fieldSet.resolve();
      this.fieldSet=fieldSet;
    }
    catch (SQLException x)
    { throw new DataException("Error reading ResultSet MetaData: "+x,x);
    }
    tuple=new ResultSetTuple(fieldSet);
    tuple.setResultSet(resultSet);
    try
    { binding=new CursorBinding(this);
    }
    catch (BindException x)
    { throw new DataException("Error creating cursor binding",x);
    }
  }
  
  @SuppressWarnings("unchecked") // Not genericized
  public SerialResultSetCursor(FieldSet fieldSet,ResultSet resultSet)
    throws DataException
  {
    this.resultSet=resultSet;
    this.fieldSet=fieldSet;
    tuple=new ResultSetTuple(fieldSet);
    tuple.setResultSet(resultSet);
    try
    { binding=new CursorBinding(this);
    }
    catch (BindException x)
    { throw new DataException("Error creating cursor binding",x);
    }
  }
  
  public SerialResultSetCursor
    (FieldSet fieldSet
    ,ResultSet resultSet
    ,LinkedTree<Integer> foldTree
    )
  { 
    this.resultSet=resultSet;
    this.fieldSet=fieldSet;
    tuple=new ResultSetTuple(fieldSet,foldTree);
    tuple.setResultSet(resultSet);
  }

  /**
   * The Connection that should be closed when this cursor is closed
   * 
   * @param connection
   */
  public void setConnection(Connection connection)
  { this.connection=connection;
  }
  
  @Override
  public FieldSet getFieldSet()
  { return fieldSet;
  }

  @Override
  public Type<?> getResultType()
  { return fieldSet.getType();
  }
  
  @Override
  public Identifier getRelationId()
  { return relationId;
  }
  
  public void setRelationId(Identifier id)
  { relationId=id;
  }
  
  /**
   * <P>Specify whether to avoid creating a new Tuple for every result row.
   * 
   * <P> If true, the same mutable Tuple object will be returned by 
   *   dataGetTuple(), but it will contain different data for each row of
   *   the resultSet.
   * 
   * <P> If false, a new, immutable Tuple object will be returned for each
   *   row of the resultSet.
   */
  public void setVolatileTuple(boolean volatileTuple)
  { this.noCopy=volatileTuple;
  }
  
  @Override
  public Tuple getTuple() throws DataException
  { 
    // XXX: Optimize by creating one new Tuple per row
    if (noCopy)
    { return tuple;
    }
    else
    { return new ArrayTuple(tuple);
    }
  }

  @Override
  public boolean next() throws DataException
  { 
    try
    { 
      boolean val=resultSet.next();
      if (val==false && autoClose)
      { resultSet.close();
      }
      return val;
    }
    catch (SQLException x)
    { throw new DataException("Error advancing ResultSet: "+x,x);
    }
  }

  @Override
  public Channel<Tuple> bind()
    throws BindException
  { return binding;
  }

  public void close()
    throws DataException
  { 
    try
    { resultSet.close();
    }
    catch (SQLException x)
    { throw new DataException("Error closing result set",x);
    }

    if (statement!=null)
    { 
      try
      { statement.close();
      }
      catch (SQLException x)
      { throw new DataException("Error closing statement",x);
      }
    }
    
    if (connection!=null)
    { 
      try
      { connection.close();
      }
      catch (SQLException x)
      { throw new DataException("Error closing connection",x);
      }
    }
  }
  
}
