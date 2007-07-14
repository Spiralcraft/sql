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
package spiralcraft.sql.converters;


import java.sql.SQLException;

/**
 * Convert an object to a Float
 */
public class IntegerConverter
	implements Converter<Integer,Object>
{
	public Integer toSql(Object value)
		throws SQLException
	{
		if (value==null)
		{ return null;
		}
    else if (value instanceof Integer)
    { return (Integer) value;
    }
    else if (value instanceof Number)
    { return new Integer(((Number) value).intValue());
    }
		else if (value instanceof String)
		{ return Integer.valueOf((String) value);
		}
		throw new SQLException("Could not safely convert object of type '"+value.getClass().getName()+"' to a Float.");
	}
  
  public Integer fromSql(Integer value)
  { return value;
  }
  
  public Class<?> getSqlClass()
  { return Integer.class;
  }    
}
