package spiralcraft.sql.types;

import spiralcraft.stream.StreamUtil;

import java.io.InputStream;
import java.io.IOException;

import java.sql.SQLException;

/**
 * Convert an object to a sql Varchar String
 */
public class VarcharConverter
	implements Converter
{
	public Object convert(Object value)
		throws SQLException
	{
		if (value==null)
		{ return null;
		}
		else if (value instanceof String)
		{ return value;
		}
    else if (value instanceof Character)
    { return value.toString();
    }
		else if (value instanceof InputStream)
		{
			try
			{ return new String(StreamUtil.readBytes((InputStream) value));
			}
			catch (IOException x)
			{ throw new SQLException("Error reading InputStream to a String during convertion to JDBC VARCHAR type: "+x.getMessage());
			}
		}
		else if (value instanceof Number)
		{ return value.toString();
		}
		else if (value instanceof byte[])
		{ return new String((byte[]) value);
		}
		throw new SQLException("Could not safely convert object of type '"+value.getClass().getName()+"' to a String.");
	}
}
