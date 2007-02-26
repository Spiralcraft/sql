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
package spiralcraft.sql.types;

import java.util.HashMap;

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

	private static HashMap<Class,SqlTypeInfo> _javaMap
    =new HashMap<Class,SqlTypeInfo>();

  private static HashMap<Integer,SqlTypeInfo> _sqlMap
    =new HashMap<Integer,SqlTypeInfo>();
  
  private static HashMap<String,SqlTypeInfo> _sqlNameMap
    =new HashMap<String,SqlTypeInfo>();
  
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
    
		mapJavaType(String.class,Types.VARCHAR);
		
		mapJavaType(Boolean.class,Types.BIT);
		mapJavaType(Character.class,Types.SMALLINT);
		mapJavaType(Byte.class,Types.TINYINT);
		mapJavaType(Short.class,Types.SMALLINT);
		mapJavaType(Integer.class,Types.INTEGER);
		mapJavaType(Long.class,Types.BIGINT);
		mapJavaType(Float.class,Types.REAL);
		mapJavaType(Double.class,Types.DOUBLE);

		mapJavaType(java.math.BigDecimal.class,Types.NUMERIC);
		
		mapJavaType(java.util.Date.class,Types.TIMESTAMP);
		mapJavaType(java.sql.Date.class,Types.DATE);
		mapJavaType(java.sql.Time.class,Types.TIME);
		mapJavaType(java.sql.Timestamp.class,Types.TIMESTAMP);

		mapJavaType(boolean.class,Types.BIT);
		mapJavaType(char.class,Types.CHAR);
		mapJavaType(byte.class,Types.TINYINT);
		mapJavaType(short.class,Types.SMALLINT);
		mapJavaType(int.class,Types.INTEGER);
		mapJavaType(long.class,Types.BIGINT);
		mapJavaType(float.class,Types.REAL);
		mapJavaType(double.class,Types.DOUBLE);
		mapJavaType(byte[].class,Types.VARBINARY);
		
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
    
    // Suggested standard mappings from JDBC docs
    
    setClass(Types.CHAR,String.class);
    setClass(Types.VARCHAR,String.class);
    setClass(Types.LONGVARCHAR,String.class);
    
    setClass(Types.BINARY,byte[].class);
    setClass(Types.VARBINARY,byte[].class);
    setClass(Types.LONGVARBINARY,byte[].class);
    
    setClass(Types.BIT,Boolean.class);
    
    setClass(Types.TINYINT,Integer.class); // Bigger than Byte
    setClass(Types.SMALLINT,Integer.class); 
    setClass(Types.INTEGER,Integer.class);
    setClass(Types.BIGINT,Long.class);
    setClass(Types.REAL,Float.class);
    setClass(Types.DOUBLE,Double.class);
    setClass(Types.FLOAT,Double.class); // Bigger than Float
    
    setClass(Types.DECIMAL,java.math.BigDecimal.class);
    setClass(Types.NUMERIC,java.math.BigDecimal.class);
    
    setClass(Types.DATE,java.sql.Date.class);
    setClass(Types.TIME,java.sql.Time.class);
    setClass(Types.TIMESTAMP,java.sql.Timestamp.class);

    setClass(Types.BLOB,java.sql.Blob.class);
    setClass(Types.CLOB,java.sql.Clob.class);
    setClass(Types.ARRAY,java.sql.Array.class);
    setClass(Types.STRUCT,java.sql.Struct.class);
    setClass(Types.REF,java.sql.Ref.class);
    
	}

	static class SqlTypeInfo
	{
		int _type;
		String _DDL;
		Converter _converter;
    Class _class;

		public SqlTypeInfo(int type,String DDL)
		{
			_type=type;
			_DDL=DDL;
		}	
	}

  /**
   * Specify a standard Java class for a specific JDBC target type.
   */ 
  public static void setClass(int sqlType,Class clazz)
  {
    SqlTypeInfo info=_sqlMap.get(sqlType);
    if (info!=null)
    { info._class=clazz;
    }
    
  }

	/**
	 * Specify a converter for a specific JDBC target type.
	 */	
	public static void setConverter(int sqlType,Converter converter)
	{
		SqlTypeInfo info=_sqlMap.get(sqlType);
		if (info!=null)
		{ info._converter=converter;
		}
		
	}
	
	public static void putSqlMap(int code,String name)
	{
		SqlTypeInfo info=new SqlTypeInfo(code,name);
		_sqlMap.put(code,info);
    _sqlNameMap.put(name,info);
	}


	public static void mapJavaType(Class clazz,int sqlType)
	{
		_javaMap.put(clazz,_sqlMap.get(sqlType));
	}
	
  public static Class getJavaClassFromSqlType(int sqlType)
  {
    SqlTypeInfo info=_sqlMap.get(sqlType);
    if (info!=null)
    { return info._class;
    }
    else
    { return null;
    }
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

