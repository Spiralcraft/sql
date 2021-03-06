//
// Copyright (c) 1998,2007 Michael Toth
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
package spiralcraft.sql.ddl;

import java.util.List;

import spiralcraft.sql.Dialect;
import spiralcraft.sql.SqlFragment;

public class DataType
    extends SqlFragment
{
  protected String typeName;
  protected Integer length;
  private Integer decimals;
  
  public DataType(Dialect dialect,String typeName,Integer length,Integer decimals)
  {
    this.typeName=typeName;
    this.length=length;
    this.decimals=decimals;
    this.dialect=dialect;
  }
  
  @Override
  public void write(StringBuilder buffer,String indent, List<?> parameterCollector)
  {
    buffer.append(typeName);
    if (length!=null)
    { 
      buffer.append("(").append(length.toString());
      if (decimals!=null)
      { buffer.append(",").append(decimals.toString());
      }
      buffer.append(")");
    }
    buffer.append(" ");
  }
}
