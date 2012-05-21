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

import spiralcraft.log.ClassLog;
import spiralcraft.sql.Dialect;

import spiralcraft.sql.ddl.TableElement;
import spiralcraft.sql.ddl.TableElementList;
import spiralcraft.sql.ddl.CreateTableStatement;
import spiralcraft.sql.ddl.AlterTableStatement;
import spiralcraft.sql.ddl.AlterTableAction;
import spiralcraft.sql.ddl.DDLStatement;

import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;



public class Table
{

  @SuppressWarnings("unused")
  private static final ClassLog log
    =ClassLog.getInstance(Table.class);
  
  private String catalogName;
  private String schemaName;
  private String name;
//  private String remarks;
  private final ArrayList<Column> columns=new ArrayList<Column>();
  private final HashMap<String,Column> columnMap=new HashMap<String,Column>();
  private final ArrayList<Index> indices=new ArrayList<Index>();
  private final ArrayList<ForeignKeyConstraint> foreignKeys=new ArrayList<ForeignKeyConstraint>();
  private final ArrayList<KeyConstraint> keys=new ArrayList<KeyConstraint>();
    
  public Table()
  {
  }
  
  /**
   * Construct a Table, reading a single row of a result set from
   *   DatabaseMetaData.getTables()
   */
  public Table(ResultSet rs)
    throws SQLException
  {
    
    catalogName=rs.getString(1);
    schemaName=rs.getString(2);
    name=rs.getString(3);
//    remarks=rs.getString(5);
    System.err.println("Table: read "+name+": catalog="+catalogName+" schema="+schemaName);
  }
  
  
  public void readMetaData(Dialect dialect,DatabaseMetaData metadata)
    throws SQLException
  {
    ResultSet rs=metadata.getColumns(catalogName,schemaName,name,"%");

    while (rs.next())
    {  addColumn(new Column(rs));
    }
    rs.close();
    
    rs=metadata.getPrimaryKeys(catalogName,schemaName,name);
    KeyConstraint primaryKey=new KeyConstraint(this,rs,true);
    rs.close();
    if (primaryKey.getColumns().length>0)
    { this.addKeyConstraint(primaryKey);
    }
    
    KeyConstraint[] uniqueConstraints
      =dialect.getUniqueConstraints(metadata.getConnection(),this);
    for (KeyConstraint constraint: uniqueConstraints)
    { 
      if (constraint.getColumns().length>0)
      { this.addKeyConstraint(constraint);
      }
    }
    
    
  }
  
  public void addKeyConstraint(KeyConstraint constraint)
  { 
    constraint.setTable(this);
    keys.add(constraint);
  }
  
  public void addForeignKeyConstraint(ForeignKeyConstraint constraint)
  { foreignKeys.add(constraint);
  }

  public void addColumn(Column column)
  { 
    column.setTable(this);
    columns.add(column);
    columnMap.put(column.getName(),column);
  }
  
  public Column getColumn(String name)
  { return columnMap.get(name);
  }
  
  public String getCatalogName()
  { return catalogName;
  }
  
  public String getSchemaName()
  { return schemaName;
  }
  
  public String getName()
  { return name;
  }

  public void setName(String name)
  { this.name=name;
  }
  
  public void setSchemaName(String schemaName)
  { this.schemaName=schemaName;
  }
  
  public Column[] getColumns()
  { return columns.toArray(new Column[columns.size()]);
  }
  
  public Index[] getIndices()
  { return indices.toArray(new Index[indices.size()]);
  }
  
  public ForeignKeyConstraint[] getForeignKeys()
  { return foreignKeys.toArray(new ForeignKeyConstraint[foreignKeys.size()]);
  }
  
  public KeyConstraint[] getKeys()
  { return keys.toArray(new KeyConstraint[keys.size()]);
  }

  public KeyConstraint getKey(KeyConstraint peer)
  { 
    for (KeyConstraint key: keys)
    { 
      if (peer.isFieldEquivalent(key))
      { return key;
      }
    }
    return null;
  }
  
  public AlterTableStatement createAlterTableStatement(AlterTableAction action)
  { return new AlterTableStatement(schemaName,name,action);
  }
  
  public List<DDLStatement> generateUpdateDDL(Dialect dialect,Table storeVersion)
  {
    ArrayList<DDLStatement> ret=new ArrayList<DDLStatement>();
    if (storeVersion==null)
    { 
      TableElementList elements=new TableElementList();
      
      for (Column column: columns)
      { 
        // log.fine("Adding column "+column);
        elements.addElement(column.generateColumnDefinition(dialect));
      }
      
      for (KeyConstraint key: keys)
      { 
        TableElement element=key.generateConstraintDefinition(dialect);
        if (element!=null)
        { elements.addElement(element);
        }
      }
      
      CreateTableStatement statement
        =new CreateTableStatement(schemaName,name,elements);
      ret.add(statement);
    }
    else
    {
      for (Column column: columns)
      { 
        ret.addAll
          (column.generateUpdateDDL
              (dialect,storeVersion.getColumn(column.getName())
              )
          );
      }

      for (KeyConstraint key: keys)
      { 
        ret.addAll
          (key.generateUpdateDDL
            (dialect,storeVersion.getKey(key)
            )
          );
      }

    }
    return ret;
  }
}
