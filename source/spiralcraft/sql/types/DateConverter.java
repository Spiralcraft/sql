package spiralcraft.sql.types;

import java.io.InputStream;
import java.io.IOException;

import java.sql.SQLException;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import java.text.SimpleDateFormat;
import java.text.ParseException;

/**
 * Converts date representations to java.sql.Date objects
 */
public class DateConverter
	implements Converter
{
  private static SimpleDateFormat _format
    =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

	public Object convert(Object value)
		throws SQLException
	{
		if (value==null)
		{ return null;
		}
    else if (value instanceof Date)
    { return value;
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
}
