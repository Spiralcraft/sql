package spiralcraft.sql.types;

import java.sql.SQLException;

/**
 * Represents a type conversion to a SQL "friendly" type from another 
 *   representation. 
 */
public interface Converter
{
	public Object convert(Object value)
		throws SQLException;
}
