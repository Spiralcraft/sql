package spiralcraft.sql.types;

import java.util.HashMap;
import java.util.Iterator;

import java.lang.reflect.Field;

import java.sql.Types;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.SQLException;

import spiralcraft.util.StringConverter;

/**
 * Provides mapping functions to convert between JDBC and Java
 *   types and SQL statement embeddable strings. 
 */
public class TypeMap
{

	private static HashMap _javaMap=new HashMap();
	private static HashMap _sqlMap=new HashMap();
  private static HashMap _sqlNameMap=new HashMap();
	private static StringConverter _classConverter
    =StringConverter.getInstance(Class.class);
	
	static
	{
		try
		{
			Field[] types=Types.class.getFields();
			
			for (int i=0;i<types.length;i++)
			{ putSqlMap(types[i].getInt(null),types[i].getName());
			}
		}
		catch (Exception x)
		{ x.printStackTrace();
		}	
		putJavaMap(String.class,Types.VARCHAR);
		
		putJavaMap(Boolean.class,Types.BIT);
		putJavaMap(Character.class,Types.SMALLINT);
		putJavaMap(Byte.class,Types.TINYINT);
		putJavaMap(Short.class,Types.SMALLINT);
		putJavaMap(Integer.class,Types.INTEGER);
		putJavaMap(Long.class,Types.BIGINT);
		putJavaMap(Float.class,Types.REAL);
		putJavaMap(Double.class,Types.DOUBLE);

		putJavaMap(java.math.BigDecimal.class,Types.NUMERIC);
		
		putJavaMap(java.util.Date.class,Types.TIMESTAMP);
		putJavaMap(java.sql.Date.class,Types.DATE);
		putJavaMap(java.sql.Time.class,Types.TIME);
		putJavaMap(java.sql.Timestamp.class,Types.TIMESTAMP);

		putJavaMap(boolean.class,Types.BIT);
		putJavaMap(char.class,Types.CHAR);
		putJavaMap(byte.class,Types.TINYINT);
		putJavaMap(short.class,Types.SMALLINT);
		putJavaMap(int.class,Types.INTEGER);
		putJavaMap(long.class,Types.BIGINT);
		putJavaMap(float.class,Types.REAL);
		putJavaMap(double.class,Types.DOUBLE);
		putJavaMap(byte[].class,Types.VARBINARY);
		
		setConverter(Types.VARCHAR,new VarcharConverter());
		setConverter(Types.CHAR,new VarcharConverter());
		setConverter(Types.LONGVARCHAR,new VarcharConverter());
    setConverter(Types.DATE,new DateConverter());
    setConverter(Types.TIMESTAMP,new TimestampConverter());
    setConverter(Types.TIME,new TimeConverter());
    setConverter(Types.TINYINT,new ByteConverter());
    setConverter(Types.SMALLINT,new ShortConverter());
    setConverter(Types.REAL,new FloatConverter());
    setConverter(Types.NUMERIC,new BigDecimalConverter());
	}

	static class SqlTypeInfo
	{
		int _type;
		String _DDL;
		Converter _converter;

		public SqlTypeInfo(int type,String DDL)
		{
			_type=type;
			_DDL=DDL;
		}	
	}

	/**
	 * Specify a converter for a specific JDBC target type.
	 */	
	public static void setConverter(int sqlType,Converter converter)
	{
		SqlTypeInfo info=(SqlTypeInfo) _sqlMap.get(new Integer(sqlType));
		if (info!=null)
		{ info._converter=converter;
		}
		
	}
	
	public static void putSqlMap(int code,String name)
	{
		SqlTypeInfo info=new SqlTypeInfo(code,name);
		_sqlMap.put(new Integer(code),info);
    _sqlNameMap.put(name,info);
	}


	public static void putJavaMap(Class clazz,int sqlType)
	{
		_javaMap.put(clazz,_sqlMap.get(new Integer(sqlType)));
	}
	
	public static String getSqlNameFromSqlType(int sqlType)
	{
		SqlTypeInfo info=(SqlTypeInfo) _sqlMap.get(new Integer(sqlType));
		if (info!=null)
		{ return info._DDL;
		}
		else
		{ return null;
		}
	}
	
  public static String getSqlNameFromJavaType(String javaType,int length)
  { return getSqlNameFromJavaType((Class) _classConverter.fromString(javaType),length);
  }
  
	public static String getSqlNameFromJavaType(Class javaType,int length)
	{
    if (javaType==String.class)
    { 
      if (length>255)
      { return "LONGVARCHAR";
      }
      else
      { return "VARCHAR";
      }
    }
    else if (javaType==byte[].class)
    { 
      if (length>255)
      { return "LONGVARBINARY";
      }
      else
      { return "VARBINARY";
      }
    }
    else
    { return getSqlNameFromJavaType(javaType);
    }

  }
  
  public static String getSqlNameFromJavaType(String javaType)
  { return getSqlNameFromJavaType((Class) _classConverter.fromString(javaType));
  }
  
  public static String getSqlNameFromJavaType(Class javaType)
  {
		SqlTypeInfo info=(SqlTypeInfo) _javaMap.get(javaType);
		if (info!=null)
		{ return info._DDL;
		}
		else
		{ return null;
		}
	}

	public static int getSqlTypeFromJavaType(String javaType)
  { return getSqlTypeFromJavaType((Class) _classConverter.fromString(javaType));
  }
  
	public static int getSqlTypeFromJavaType(Class javaType)
	{
		SqlTypeInfo info=(SqlTypeInfo) _javaMap.get(javaType);
		if (info!=null)
		{ return info._type;
		}
		else
		{ return Types.OTHER;
		}
	}
	
	public static int getSqlTypeFromJavaType(String javaType,int length)
  { return getSqlTypeFromJavaType((Class) _classConverter.fromString(javaType),length);
  }

  public static int getSqlTypeFromJavaType(Class javaType,int length)
  {
    if (javaType==String.class)
    { 
      if (length>255)
      { return Types.LONGVARCHAR;
      }
      else
      { return Types.VARCHAR;
      }
    }
    else if (javaType==byte[].class)
    { 
      if (length>255)
      { return Types.LONGVARBINARY;
      }
      else
      { return Types.VARBINARY;
      }
    }
    else
    { return getSqlTypeFromJavaType(javaType);
    }
  }

  public static Integer getSqlTypeFromSqlName(String name)
  { 
		SqlTypeInfo info=(SqlTypeInfo) _sqlNameMap.get(name);
		if (info!=null)
		{ return new Integer(info._type);
		}
		else
		{ return null;
		}
	} 

	public static String convertObjectToDMLString(Object val)
	{
		if (val==null)
		{ return "NULL";
		}
		SqlTypeInfo info=(SqlTypeInfo) _javaMap.get(val.getClass());
		int sqlType=info._type;
		switch (sqlType)
		{
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
				return "'"+val.toString()+"'";
			case Types.INTEGER:
			case Types.NUMERIC:
			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.BIGINT:
			case Types.REAL:
			case Types.DOUBLE:
				return val.toString();
			case Types.DATE:
				if (val instanceof java.util.Date)
				{	return "'"+new Date( ((java.util.Date) val).getTime() ).toString()+"'";
				}
				else
				{ return "'"+ ((Date) val).toString() +"'";
				}
			case Types.TIME:
				return "'"+ ((Time) val).toString() +"'";
			case Types.TIMESTAMP:
				return "'"+ ((Timestamp) val).toString() +"'";
			case Types.BIT:
				return ((Boolean) val).equals(Boolean.TRUE)?"true":"false";
			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
				return new String((byte[]) val);
			default:
				return null;
		}
		
	}
	
	/**
	 * Obtain a converter that converts an arbitrary object
	 *   to a Java type that corresponds to a standard JDBC
	 *   type.
	 *@return An appropriate converter or null if none is available.
	 */
	public static Converter getConverterForSqlType(int sqlType)
	{
		SqlTypeInfo info=(SqlTypeInfo) _sqlMap.get(new Integer(sqlType));
		if (info!=null)
		{ return info._converter;
		}
		else
		{ return null;
		}
	}

  public static Object convertToSqlType(Object value,int sqlType)
    throws SQLException
  {
    Converter converter=getConverterForSqlType(sqlType);
    if (converter==null)
    { return value;
    }
    else
    { return converter.convert(value);
    }
  }
}

