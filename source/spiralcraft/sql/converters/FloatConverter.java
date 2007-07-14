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
public class FloatConverter
	implements Converter<Float,Object>
{
	public Float toSql(Object value)
		throws SQLException
	{
		if (value==null)
		{ return null;
		}
    else if (value instanceof Float)
    { return (Float) value;
    }
    else if (value instanceof Number)
    { return new Float(((Number) value).floatValue());
    }
		else if (value instanceof String)
		{ return Float.valueOf((String) value);
		}
		throw new SQLException("Could not safely convert object of type '"+value.getClass().getName()+"' to a Float.");
	}
  
  public Float fromSql(Float value)
  { return value;
  }
  
  public Class<?> getSqlClass()
  { return Float.class;
  }    
}
