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

import spiralcraft.data.spi.AbstractTuple;

import spiralcraft.data.FieldSet;
import spiralcraft.data.Tuple;
import spiralcraft.data.DataException;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A Tuple that retrieves data from a ResultSet. If the ResultSet is advanced,
 *   the Tuple's data will change.
 */
public class ResultSetTuple
  extends AbstractTuple
  implements Tuple
{
  private ResultSet resultSet;
  
  public ResultSetTuple(FieldSet fieldSet)
  { super(fieldSet);
  }
  
  /**
   * Replace the result set with the specified result set. Used to compose a single
   *   data stream from multiple SQL sources of the same type
   */
  public void setResultSet(ResultSet resultSet)
  { this.resultSet=resultSet;
  }
  
  public Object get(int index)
    throws DataException
  {
    try
    { return resultSet.getObject(index+1);
    }
    catch (SQLException x)
    { throw new DataException("Error reading result set: "+x,x);
    }

  }

  /**
   *@return true, because this Tuple is simply a view of the ResultSet which can
   *  be advanced at any time
   */
  public boolean isMutable()
  { return true;
  }
}
