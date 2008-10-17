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

/**
 * A SQL "Search Condition" or "OR" expression
 */
public class SearchCondition
  extends BooleanCondition
{
  private final BooleanCondition searchCondition;
  private final BooleanCondition booleanTerm;
  
  
  public SearchCondition(BooleanCondition searchCondition,BooleanCondition booleanTerm)
  { 
    this.searchCondition=searchCondition;
    this.booleanTerm=booleanTerm;
  }
  
  @Override
  public int getPrecedence()
  { return 1;
  }
  
  @Override
  public void write(StringBuilder buffer,String indent, List<?> parameterCollector)
  {
    searchCondition.write(buffer,indent, parameterCollector);
    buffer.append("\r\n").append(indent).append("OR ");
    indent=indent+"  ";
    booleanTerm.write(buffer,indent, parameterCollector);
  }
}
