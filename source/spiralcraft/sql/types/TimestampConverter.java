package spiralcraft.sql.types;

import java.io.InputStream;
import java.io.IOException;

import java.sql.SQLException;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import java.text.SimpleDateFormat;
import java.text.ParseException;

import java.util.TimeZone;

/**
 * Convert a representation of a timestamp to a java.sql.Timestamp
 */
public class TimestampConverter
	implements Converter
{
  private static SimpleDateFormat _format
    =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
  { _format.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

	public Object convert(Object value)
		throws SQLException
	{
		if (value==null)
		{ return null;
		}
    else if (value instanceof Timestamp)
    { return new java.util.Date(((Timestamp) value).getTime());
    }
		else if (value instanceof java.util.Date)
		{ return value;
		}
    else if (value instanceof String)
    { 
      try
      { 
        synchronized (_format)
        { return _format.parse((String) value);
        }
      }
      catch (ParseException x)
      { throw new SQLException("Invalid date format. Must be 'yyyy-MM-dd HH:mm:ss'");
      }
    }
		throw new SQLException("Could not safely convert object of type '"+value.getClass().getName()+"' to a Date.");
	}
}
