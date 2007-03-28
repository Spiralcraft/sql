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

import spiralcraft.sql.SqlFragment;

import java.util.ArrayList;

public class TableElementList
    extends SqlFragment
{
  
  private ArrayList<TableElement> elements=new ArrayList<TableElement>();

  public void addElement(TableElement element)
  { elements.add(element);
  }
  
  public void write(StringBuilder buffer,String indent)
  {
    buffer.append("\r\n").append(indent);
    buffer.append("(");
    boolean first=true;
    for (TableElement element: elements)
    { 
      if (first)
      { first=false;
      }
      else
      { buffer.append("\r\n").append(indent).append(",");
      }
      element.write(buffer,indent);
    }
    buffer.append("\r\n").append(indent).append(")");
  }
}
