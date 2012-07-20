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

import java.sql.Date;


import java.text.SimpleDateFormat;
import java.text.ParseException;

/**
 * Converts date representations to java.sql.Date objects
 */
public class DateConverter
	extends Converter<Date,Object>
{
  private static SimpleDateFormat _format
    =new SimpleDateFormat("yyyy-MM-dd");

	@Override
  public Date toSql(Object value)
		throws SQLException
	{
		if (value==null)
		{ return null;
		}
    else if (value instanceof Date)
    { return (Date) value;
    }
		else if (value instanceof java.util.Date)
		{ return new Date( ((java.util.Date) value).getTime());
		}
    else if (value instanceof String)
    { 
      try
      { return new Date(_format.parse((String) value).getTime());
      }
      catch (ParseException x)
      { throw new SQLException("Invalid date format. Must be 'yyyy-MM-dd HH:mm:ss'");
      }
    }
		throw new SQLException("Could not safely convert object of type '"+value.getClass().getName()+"' to a Date.");
	}
  
  @Override
  public java.util.Date fromSql(Date date)
  { return date;
  }

  @Override
  public Class<?> getSqlClass()
  { return Date.class;
  }    
}
