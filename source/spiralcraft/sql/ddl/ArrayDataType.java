package spiralcraft.sql.ddl;

import java.util.List;

import spiralcraft.sql.Dialect;

public class ArrayDataType
  extends DataType
{

  private final DataType componentType;
  
  public ArrayDataType(
    Dialect dialect,
    String typeName,
    Integer length,
    DataType componentType
    )
  { 
    super(dialect, typeName, length, null);
    this.componentType=componentType;
  }

  @Override
  public void write(StringBuilder buffer,String indent, List<?> parameterCollector)
  {
    componentType.write(buffer,indent,parameterCollector);
    buffer.append(typeName);
    if (length!=null)
    { 
      buffer.append("[").append(length.toString());
      buffer.append("]");
    }
    buffer.append(" ");
  }  
}
