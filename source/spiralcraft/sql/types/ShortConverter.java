package spiralcraft.sql.types;

import java.io.InputStream;
import java.io.IOException;

import java.sql.SQLException;

/**
 * Convert an object to a Short
 */
public class ShortConverter
	implements Converter
{
	public Object convert(Object value)
		throws SQLException
	{
		if (value==null)
		{ return null;
		}
    else if (value instanceof Short)
    { return value;
    }
    else if (value instanceof Number)
    { return new Short(((Number) value).shortValue());
    }
		else if (value instanceof String)
		{ return Short.valueOf((String) value);
		}
		throw new SQLException("Could not safely convert object of type '"+value.getClass().getName()+"' to a Short.");
	}
}
