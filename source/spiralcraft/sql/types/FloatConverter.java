package spiralcraft.sql.types;


import java.io.InputStream;
import java.io.IOException;

import java.sql.SQLException;

/**
 * Convert an object to a Float
 */
public class FloatConverter
	implements Converter
{
	public Object convert(Object value)
		throws SQLException
	{
		if (value==null)
		{ return null;
		}
    else if (value instanceof Float)
    { return value;
    }
    else if (value instanceof Number)
    { return new Float(((Number) value).floatValue());
    }
		else if (value instanceof String)
		{ return Float.valueOf((String) value);
		}
		throw new SQLException("Could not safely convert object of type '"+value.getClass().getName()+"' to a Float.");
	}
}
