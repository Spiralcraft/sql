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
package spiralcraft.sql.model;

import spiralcraft.sql.Dialect;
import spiralcraft.sql.SqlType;


import spiralcraft.sql.ddl.ColumnDefinition;
import spiralcraft.sql.ddl.AlterTableStatement;
import spiralcraft.sql.ddl.AddColumnDefinition;
import spiralcraft.sql.ddl.DDLStatement;

import spiralcraft.sql.dml.ValueExpression;
import spiralcraft.sql.dml.IdentifierChain;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Column
{

  private Table table;
  private String name;
  private SqlType<?> type;
  private Integer length;
  private boolean nullable;
  private Integer decimalDigits;
  private int position;
  private ValueExpression valueExpression;
  
  
  public Column()
  { 
  }
  
  /**
   * Create a Column from a row in the ResultSet returned by DatabaseMetaData.getColumns()
   */
  public Column(ResultSet rs)
    throws SQLException
  {
    System.err.println("Column: reading "+rs.getString(4));
    
    name=rs.getString(4);
    type=SqlType.getSqlType(rs.getInt(5));
    if (rs.getObject(7)!=null)
    { length=rs.getInt(7);
    }
    if (rs.getObject(9)!=null)
    { decimalDigits=rs.getInt(9);
    }
    position=rs.getInt(17)-1;
    nullable=rs.getString(18)==null || rs.getString(18).equals("YES");
  }
  
  /**
   * 
   * @return the Table to which this field belongs.
   */
  public Table getTable()
  { return table;
  }
  
  public void setTable(Table table)
  { this.table=table;
  }
  
  /**
   * 
   * @return the ordinal position of the column within the Table
   */
  public int getPosition()
  { return position;
  }

  public void setPosition(int position)
  { this.position=position;
  }

  /**
   * 
   * @return the column name
   */
  public String getName()
  { return name;
  }
  
  public void setName(String name)
  { this.name=name;
  }
  
  
  
  /**
   * 
   * @return the SQL datatype, from java.sql.Types
   */
  public SqlType<?> getType()
  { return type;
  }

  public void setType(SqlType<?> type)
  { this.type=type;
  }
  
  
  /**
   * 
   * @return the maxumum number of characters or digits
   */
  public int getLength()
  { return length;
  }
  
  public void setLength(int length)
  { this.length=length;
  }
  
  /**
   * 
   * @return the number of fractional digits
   */
  public int getDecimalDigits()
  { return decimalDigits;
  }
  
  public void setDecimalDigits(int decimalDigits)
  { this.decimalDigits=decimalDigits;
  }
  
  
  public boolean isNullable()
  { return nullable;
  }
  
  public void setNullable(boolean val)
  { this.nullable=val;
  }
  
  

  
  public ColumnDefinition generateColumnDefinition(Dialect dialect)
  { return new ColumnDefinition(name,type.createDDL(length,decimalDigits));
  }
  
  public synchronized ValueExpression getValueExpression()
  { 
    if (valueExpression==null)
    { valueExpression=new IdentifierChain(name);
    }
    return valueExpression;
  }
  
  public List<DDLStatement> generateUpdateDDL(Dialect dialect,Column storeVersion)
  {
    ArrayList<DDLStatement> ret=new ArrayList<DDLStatement>();
    if (storeVersion==null)
    { 
      AlterTableStatement statement
        =table.createAlterTableStatement
          (new AddColumnDefinition
            (generateColumnDefinition(dialect)
            )
          );
      ret.add(statement);
    }
    else
    {
    }
    return ret;
  }  
  
}
