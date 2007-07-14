//
//Copyright (c) 1998,2007 Michael Toth
//Spiralcraft Inc., All Rights Reserved
//
//This package is part of the Spiralcraft project and is licensed under
//a multiple-license framework.
//
//You may not use this file except in compliance with the terms found in the
//SPIRALCRAFT-LICENSE.txt file at the top of this distribution, or available
//at http://www.spiralcraft.org/licensing/SPIRALCRAFT-LICENSE.txt.
//
//Unless otherwise agreed to in writing, this software is distributed on an
//"AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.
//
package spiralcraft.sql;

import spiralcraft.sql.converters.Converter;
import spiralcraft.sql.ddl.DataType;


import java.sql.Types;
import java.util.HashMap;

public abstract class SqlType<T>
{
  private static final HashMap<Integer,SqlType<?>> CANONICAL_MAP
    =new HashMap<Integer,SqlType<?>>();

  private static final HashMap<Class<?>,SqlType<?>> CONVERSION_MAP
    =new HashMap<Class<?>,SqlType<?>>();
  
  static {
    mapType(new spiralcraft.sql.types.ArrayType());
    mapType(new spiralcraft.sql.types.BigIntType());
    mapType(new spiralcraft.sql.types.BinaryType());
    mapType(new spiralcraft.sql.types.BitType());
    mapType(new spiralcraft.sql.types.BlobType());
    mapType(new spiralcraft.sql.types.CharType());
    mapType(new spiralcraft.sql.types.ClobType());
    mapType(new spiralcraft.sql.types.DateType());
    mapType(new spiralcraft.sql.types.DecimalType());
    mapType(new spiralcraft.sql.types.DoubleType());
    mapType(new spiralcraft.sql.types.FloatType());
    mapType(new spiralcraft.sql.types.IntegerType());
    mapType(new spiralcraft.sql.types.LongVarBinaryType());
    mapType(new spiralcraft.sql.types.LongVarCharType());
    mapType(new spiralcraft.sql.types.NumericType());
    mapType(new spiralcraft.sql.types.RealType());
    mapType(new spiralcraft.sql.types.RefType());
    mapType(new spiralcraft.sql.types.SmallIntType());
    mapType(new spiralcraft.sql.types.StructType());
    mapType(new spiralcraft.sql.types.TimestampType());
    mapType(new spiralcraft.sql.types.TimeType());
    mapType(new spiralcraft.sql.types.TinyIntType());
    mapType(new spiralcraft.sql.types.VarBinaryType());
    mapType(new spiralcraft.sql.types.VarCharType());
    

    mapConversion(String.class,Types.VARCHAR);
    
    mapConversion(Boolean.class,Types.BIT);
    mapConversion(Character.class,Types.SMALLINT);
    mapConversion(Byte.class,Types.TINYINT);
    mapConversion(Short.class,Types.SMALLINT);
    mapConversion(Integer.class,Types.INTEGER);
    mapConversion(Long.class,Types.BIGINT);
    mapConversion(Float.class,Types.REAL);
    mapConversion(Double.class,Types.DOUBLE);

    mapConversion(java.math.BigDecimal.class,Types.NUMERIC);
    
    mapConversion(java.util.Date.class,Types.TIMESTAMP);
    mapConversion(java.sql.Date.class,Types.DATE);
    mapConversion(java.sql.Time.class,Types.TIME);
    mapConversion(java.sql.Timestamp.class,Types.TIMESTAMP);

    mapConversion(boolean.class,Types.BIT);
    mapConversion(char.class,Types.CHAR);
    mapConversion(byte.class,Types.TINYINT);
    mapConversion(short.class,Types.SMALLINT);
    mapConversion(int.class,Types.INTEGER);
    mapConversion(long.class,Types.BIGINT);
    mapConversion(float.class,Types.REAL);
    mapConversion(double.class,Types.DOUBLE);
    mapConversion(byte[].class,Types.VARBINARY);
    
  }
  
  private static void mapType(SqlType<?> type)
  { 
    CANONICAL_MAP.put
      (type.getTypeId()
      ,type
      );
  }
  
  private static void mapConversion(Class<?> clazz,int typeId)
  { 
    CONVERSION_MAP.put
      (clazz
      ,getSqlType(typeId)
      );
  }
  
  public static final SqlType<?> getSqlType(int typeId)
  { return CANONICAL_MAP.get(typeId);
  }

  
  public static final SqlType<?> getStandardSqlType(Class<?> clazz)
  { return CONVERSION_MAP.get(clazz);
  }
  
  protected final int typeId;
  protected final Class<T> sqlClass;
  protected final String name;
  protected String ddl;
  protected Converter<T,?> converter;
  
  public SqlType(int typeId,Class<T> sqlClass,String name)
  { 
    this.typeId=typeId;
    this.sqlClass=sqlClass;
    this.name=name;
    this.ddl=name;
  }
  
  /**
   * @return The canonical name of this SqlType, which corresponds to the field
   *   name in java.sql.Types associated with the JDBC type id.
   */
  public String getName()
  { return name;
  }
  
  public int getTypeId()
  { return typeId;
  }
  
  public Class<T> getSqlClass()
  { return sqlClass;
  }
  
  public Converter<T,?> getConverter()
  { return converter;
  } 
  
  /**
   * @return The DataType definiton DDL fragment for this Type. Defaults to the
   *   field name of the java.sql.Types class associated with this type.
   */
  public DataType createDDL(Integer length,Integer decimals)
  { return new DataType(ddl,length,decimals);
  }
  
}
