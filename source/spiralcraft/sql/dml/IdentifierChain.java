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

package spiralcraft.sql.dml;

import java.util.List;

import spiralcraft.util.ArrayUtil;


public class IdentifierChain
  extends ValueExpressionPrimary
{
  private String identifier;
  private IdentifierChain rest;
  
  public IdentifierChain(String identifier)
  { this.identifier=identifier;
  }
  
  public IdentifierChain(String identifier,String ... rest)
  { 
    this.identifier=identifier;
    if (rest!=null && rest.length>0)
    { 
      this.rest=new IdentifierChain
        (rest[0],(String[]) ArrayUtil.truncateBefore(rest,1));
    }
  }
  
  
  public void write(StringBuilder buffer,String indent, List parameterCollector)
  {
    buffer.append("\"").append(identifier).append("\"");
    if (rest!=null)
    {
      buffer.append(".");
      rest.write(buffer,indent, parameterCollector);
    }
    else
    { buffer.append(" ");
    }
  }
}  

