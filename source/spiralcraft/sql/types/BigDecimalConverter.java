package spiralcraft.sql.types;

import java.io.InputStream;
import java.io.IOException;

import java.sql.SQLException;

import java.math.BigDecimal;

/**
 * Convert an object to a BigDecimal
 */
public class BigDecimalConverter
	implements Converter
{
	public Object convert(Object value)
		throws SQLException
	{
		if (value==null)
		{ return null;
		}
    else if (value instanceof BigDecimal)
    { return value;
    }
    else if (value instanceof Number)
    { return new BigDecimal(((Number) value).longValue());
    }
		else if (value instanceof String)
		{ return new BigDecimal((String) value);
		}
		throw new SQLException("Could not safely convert object of type '"+value.getClass().getName()+"' to a BigDecimal.");
	}
}
