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

import java.sql.Time;

import java.text.SimpleDateFormat;
import java.text.ParseException;

/**
 * Convert a representation of a time to a java.sql.Time
 */
public class TimeConverter
	implements Converter<Time,Object>
{
  private static SimpleDateFormat _format
    =new SimpleDateFormat("HH:mm:ss");

	public Time toSql(Object value)
		throws SQLException
	{
		if (value==null)
		{ return null;
		}
    else if (value instanceof Time)
    { return (Time) value;
    }
		else if (value instanceof java.util.Date)
		{ return new Time( ((java.util.Date) value).getTime());
		}
    else if (value instanceof String)
    { 
      try
      { return new Time(_format.parse((String) value).getTime());
      }
      catch (ParseException x)
      { throw new SQLException("Invalid time format. Must be 'HH:mm:ss'");
      }
    }
		throw new SQLException("Could not safely convert object of type '"+value.getClass().getName()+"' to a Time.");
	}
  
  public java.util.Date fromSql(Time value)
  { return value;
  }
  
  public Class getSqlClass()
  { return Time.class;
  }  
}
