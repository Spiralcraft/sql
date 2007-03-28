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
import spiralcraft.data.Tuple;
import spiralcraft.data.transport.SerialCursor;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SerialResultSetCursor
  implements SerialCursor
{
  protected final FieldSet fieldSet;
  protected final ResultSetTuple tuple;
  protected ResultSet resultSet;
  protected boolean autoClose=true;
  
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
  }
  
  public SerialResultSetCursor(FieldSet fieldSet,ResultSet resultSet)
  {
    this.resultSet=resultSet;
    this.fieldSet=fieldSet;
    tuple=new ResultSetTuple(fieldSet);
    tuple.setResultSet(resultSet);
  }
  
  public FieldSet dataGetFieldSet()
  { return fieldSet;
  }

  public Tuple dataGetTuple() throws DataException
  { return tuple;
  }

  public boolean dataNext() throws DataException
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
  
}
