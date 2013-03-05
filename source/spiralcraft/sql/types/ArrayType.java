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
package spiralcraft.sql.types;

import spiralcraft.sql.Dialect;
import spiralcraft.sql.SqlType;
import spiralcraft.sql.ddl.ArrayDataType;
import spiralcraft.sql.ddl.DataType;

import java.sql.Types;

import java.sql.Array;
import java.util.HashMap;

public class ArrayType
  extends SqlType<Array>
{
  
  private static final HashMap<SqlType<?>,ArrayType> map
    =new HashMap<SqlType<?>,ArrayType>();

  
  public static synchronized ArrayType forType(SqlType<?> type)
  {
    ArrayType ret=map.get(type);
    if (ret==null)
    {
      ret=new ArrayType(type);
      map.put(type,ret);
    }
    return ret;
  }
  
  private final SqlType<?> componentType;
  
  public ArrayType()
  { 
    super(Types.ARRAY,Array.class,"ARRAY");
    componentType=null;
  }
  
  protected ArrayType(SqlType<?> componentType)
  { 
    super(Types.ARRAY,Array.class,"ARRAY");
    this.componentType=componentType;
  }
  
  /**
   * @return The DataType definiton DDL fragment for this Type. Defaults to the
   *   field name of the java.sql.Types class associated with this type.
   */
  @Override
  public DataType createDDL(Dialect dialect,Integer length,Integer decimals)
  { return new ArrayDataType(dialect,ddl,length,componentType.createDDL(dialect,null,null));
  }  
  
}
