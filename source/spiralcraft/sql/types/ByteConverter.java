package spiralcraft.sql.types;


import java.io.InputStream;
import java.io.IOException;

import java.sql.SQLException;

/**
 * Convert an object to a Byte
 */
public class ByteConverter
	implements Converter
{
	public Object convert(Object value)
		throws SQLException
	{
		if (value==null)
		{ return null;
		}
    else if (value instanceof Byte)
    { return value;
    }
    else if (value instanceof Number)
    { return new Byte(((Number) value).byteValue());
    }
		else if (value instanceof String)
		{ return Byte.valueOf((String) value);
		}
		throw new SQLException("Could not safely convert object of type '"+value.getClass().getName()+"' to a Byte.");
	}
}
