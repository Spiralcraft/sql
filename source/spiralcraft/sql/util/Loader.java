package spiralcraft.sql.util;

import spiralcraft.data.Key;
import spiralcraft.data.DataException;

import spiralcraft.data.tabfile.DataHandler;
import spiralcraft.data.tabfile.Parser;
import spiralcraft.data.tabfile.ParseException;
import spiralcraft.data.tabfile.FieldInfo;

import spiralcraft.stream.Resource;
import spiralcraft.stream.Resolver;
import spiralcraft.stream.StreamUtil;
import spiralcraft.stream.UnresolvableURIException;

import spiralcraft.util.ArrayUtil;


import spiralcraft.sql.Constants;
import spiralcraft.sql.DriverAgent;
import spiralcraft.sql.ResourceConnectionInfo;

import spiralcraft.sql.types.TypeMap;

import spiralcraft.exec.Executable;
import spiralcraft.exec.ExecutionContext;

import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.File;
import java.io.PrintWriter;
import java.io.OutputStreamWriter;
import java.io.FileWriter;

import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.Types;
import java.sql.Timestamp;
import java.sql.ResultSet;

import java.util.StringTokenizer;

import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.HashMap;

import java.util.zip.GZIPInputStream;

import java.net.URI;

import java.math.BigDecimal;

import java.text.DateFormat;
import java.text.SimpleDateFormat;


/**
 * Loads a SQL database from an input file
 */
public class Loader
  implements Executable
{

  private Resource _schemaResource;
  private Resource _connectionResource;
  private File _logFile;
  private PrintWriter _logWriter=new PrintWriter(new OutputStreamWriter(System.out),true);
  private String _tableName;
  private Connection _connection;
  private PreparedStatement _statement;
  private int _count;
  private int _transactionSize=1000;
  private int _statusInterval=100;
  private int _skipCount=0;
  private List _updateKey=null;
  private List _insertKey=null;
  private int[] _paramMap=null;
  private int[] _typeMap=null;
  private int[] _insertKeyMapping=null;
  private boolean _checkKey=false;
  private File _discardFile=null;
  private PrintWriter _discardWriter;
  private boolean _noTypes;
  private char _delimiter=',';
  private boolean _delete=false;
  private boolean _gzip=false;
  private ArrayList _inputResources=new ArrayList();
  private boolean _truncate=false;
  private boolean _autoCommit=false;
  private String _connectionSetupSql;
  private Parser parser;
  private HashMap<Key,Key> _keyMap=new HashMap<Key,Key>();
  private DateFormat _dateFormat
    =new SimpleDateFormat(Constants.JDBC_TIMESTAMP_ESCAPE_FORMAT);
  { _dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  
  public void execute(ExecutionContext context,String[] args)
  {
    if (args.length==0)
    {
      System.err.println(usage());
      return;
    }
    try
    {
  
      for (int i=0;i<args.length;i++)
      {
        if (args[i].startsWith("-"))
        { 
          String option=args[i].substring(1).toLowerCase();
          if (option.equals("table"))
          { setTableName(args[++i]);
          }
          else if (option.equals("database"))
          { 
            try
            {
              setConnectionResource
                (Resolver.getInstance().resolve
                  (context.canonicalize(URI.create(args[++i])))
                );
            }
            catch (UnresolvableURIException x)
            { throw new RuntimeException("Error resolving "+args[i],x);
            }
          }
          else if (option.equals("input"))
          { 
            try
            {
              addInputResource
                (Resolver.getInstance().resolve
                  (context.canonicalize
                    (URI.create(args[++i])))
                );
            }
            catch (UnresolvableURIException x)
            { throw new RuntimeException("Error resolving "+args[i],x);
            }
          }
          else if (option.equals("schema"))
          { 
            try
            {
              setSchemaResource
                (Resolver.getInstance().resolve
                  (context.canonicalize
                    (URI.create(args[++i]))
                  )
                );
            }
            catch (UnresolvableURIException x)
            { throw new RuntimeException("Error resolving "+args[i],x);
            }
          }
          else if (option.equals("skip"))
          { setSkipCount(Integer.parseInt(args[++i]));
          }
          else if (option.equals("gzip"))
          { setGzip(true);
          }
          else if (option.equals("updatekey"))
          { 
            List list=new LinkedList();
            StringTokenizer tok=new StringTokenizer(args[++i],",");
            while (tok.hasMoreTokens())
            { list.add(tok.nextToken());
            }
            setUpdateKey(list);
          }
          else if (option.equals("insertkey"))
          { 
            List list=new LinkedList();
            StringTokenizer tok=new StringTokenizer(args[++i],",");
            while (tok.hasMoreTokens())
            { list.add(tok.nextToken());
            }
            setInsertKey(list);
          }
          else if (option.equals("checkkey"))
          { setCheckKey(true);
          }
          else if (option.equals("discardfile"))
          { setDiscardFile(new File(args[++i]));
          }
          else if (option.equals("notypes"))
          { setNoTypes(true);
          }
          else if (option.equals("delimiter"))
          { setDelimiter(args[++i].charAt(0));
          }
          else if (option.equals("delete"))
          { setDelete(true);
          }
          else if (option.equals("log"))
          { setLogFile(new File(args[++i]));
          }
          else if (option.equals("truncate"))
          { setTruncate(true);
          }
          else if (option.equals("nobatch"))
          { setAutoCommit(true);
          }
          else if (option.equals("connectionsetupsql"))
          { setConnectionSetupSql(args[++i]);
          }
          else if (option.equals("transactionsize"))
          { setTransactionSize(Integer.parseInt(args[++i]));
          }
          else if (option.equals("statusinterval"))
          { setStatusInterval(Integer.parseInt(args[++i]));
          }
          else
          { 
            System.err.println("Unknown option '-"+option+"'");
            System.err.println(usage());
            return;
          }
        }
        else
        { addInputResource(Resolver.getInstance().resolve(URI.create(args[i])));
        }
      }
      run();
    }
    catch (IOException x)
    { x.printStackTrace();
    }
  }

  public static String usage()
  {
    try
    { return 
        new String
          (StreamUtil.readBytes
            (Loader.class.getResourceAsStream("TabfileBulkLoad.usage.txt")
            )
          );
    }
    catch (Exception x)
    { return ""; 
    }
  }

  public void setParser(Parser parser)
  { this.parser=parser;
  }
  
  public void setTransactionSize(int val)
  { _transactionSize=val;
  }
  
  public void setStatusInterval(int val)
  { _statusInterval=val;
  }

  public void setTruncate(boolean val)
  { _truncate=val;
  }
  
  public void setGzip(boolean val)
  { _gzip=val;
  }

  public void setDelete(boolean delete)
  { _delete=delete;
  }

  public void setAutoCommit(boolean val)
  { _autoCommit=val;
  }
  
  public void setConnectionSetupSql(String sql)
  { _connectionSetupSql=sql;
  }
  
  
  public void setLogFile(File log)
    throws IOException
  { _logWriter=new PrintWriter(new FileWriter(log,true),true);
  }

  public void run()
  {
    if (_tableName==null)
    { throw new RuntimeException("No table name specified");
    }
    if (_connectionResource==null)
    { throw new RuntimeException("No database specified");
    }
    if (_inputResources.size()==0)
    { throw new RuntimeException("No input files specified");
    }
    
    
    try
    { 
      if (_discardFile!=null)
      { 
        if (!_discardFile.exists())
        { _discardWriter=new PrintWriter(new FileWriter(_discardFile));
        }
        else 
        { 
          throw new IOException
            ("Discard file '"
            +_discardFile.getName()
            +"' already exists."
            );
        }

      }

      if (_inputResources.size()>0)
      {
        Iterator it=_inputResources.iterator();
        while (it.hasNext())
        { load((Resource) it.next());
        }
      }
      
    }
    catch (IOException x)
    { x.printStackTrace();
    }
    catch (ParseException x)
    { x.printStackTrace();
    }
    catch (SQLException x)
    { x.printStackTrace();
    }
    catch (DataException x)
    { x.printStackTrace();
    }
    finally
    {
      if (_discardWriter!=null)
      {
        _discardWriter.flush();
        _discardWriter.close();
        _discardWriter=null;
      }
    }
    
  }

  private void load(Resource resource)
    throws IOException,ParseException,SQLException,DataException
  {
    
    if (parser==null)
    { 
      System.err.println("Creating parser");
      parser=new Parser();
      parser.setReadHeader(true);
      parser.setNoTypes(_noTypes);
      parser.setDelimiter(_delimiter);
    }

    System.err.println("Checking data in "+resource.getURI()+"...");

    // Prescan data for syntax
    VerifyDataHandler verifyHandler=new VerifyDataHandler();
    if (_schemaResource!=null)
    { 
      parser.readHeaderFromStream
        (_schemaResource.getInputStream());
    }

    if (_gzip)
    { 
      parser.parse
        (new BufferedInputStream
          (new GZIPInputStream
            (resource.getInputStream()
            )
          )
        ,verifyHandler
        )
        ;
    }
    else
    { 
      parser.parse
        (new BufferedInputStream
          (resource.getInputStream()
          )
        ,verifyHandler
        )
        ;
    }

    _logWriter.println(verifyHandler.getCount()+" rows checked.");

    
    // Load data
    DataHandler handler=new SqlDataHandler();

    if (_gzip)
    { 
      parser.parse
        (new BufferedInputStream
          (new GZIPInputStream
            (resource.getInputStream()
            )
          )
        ,handler
        )
        ;
    }
    else
    { 
      parser.parse
        (new BufferedInputStream
          (resource.getInputStream()
          )
          ,handler
        )
        ;
    }

    _connection.commit();
    if (!_checkKey)
    { _logWriter.println("Updated "+_count);
    }
    else
    { _logWriter.println("Queried "+_count);
    }
    _connection.close();
  }
  
  
  public void setConnectionResource(Resource resource)
  { _connectionResource=resource;
  }

  public void setTableName(String tableName)
  { _tableName=tableName;
  }

  public void setInputResource(Resource inputResource)
  { 
    _inputResources.clear();
    _inputResources.add(inputResource);
  }

  public void addInputResource(Resource inputResource)
  { _inputResources.add(inputResource);
  }

  public void setSchemaResource(Resource schemaResource)
  { _schemaResource=schemaResource;
  }

  public void setSkipCount(int skipCount)
  { _skipCount=skipCount;
  }
  
  public void setUpdateKey(List updateKey)
  { _updateKey=updateKey;
  }

  public void setInsertKey(List insertKey)
  { _insertKey=insertKey;
  }

  public void setCheckKey(boolean checkKey)
  { _checkKey=checkKey;
  }

  public void setDiscardFile(File discardFile)
  { _discardFile=discardFile;
  }

  public void setNoTypes(boolean noTypes)
  { _noTypes=noTypes;
  }

  public void setDelimiter(char delimiter)
  { _delimiter=delimiter;
  }

  class VerifyDataHandler
    implements DataHandler
  {
    public void handleFieldInfo(FieldInfo[] fields)
    {
      System.err.println(ArrayUtil.format(fields,",","\""));
      _schema=fields;
      
      if (_insertKey!=null)
      { 
        _insertKeyMapping=new int[_insertKey.size()];
        for (int i=0;i<_insertKey.size();i++)
        { 
          String fieldName=(String) _insertKey.get(i);
          boolean found=false;
          for (int j=0;j<fields.length && !found;j++)
          {
            if (fields[j].getName().equals(fieldName))
            { 
              _insertKeyMapping[i]=j;
              found=true;
            }
          }
          if (!found)
          { throw new RuntimeException("Key field '"+fieldName+"' not found");
          }
          
        }
        
      }
    }

    public void handleData(Object[] data)
    { 
      if (data.length>_schema.length)
      { 
        throw new RuntimeException
          ("Too many values: "+ArrayUtil.format(data,",",null));
      }
      _count++;
    }
    
    public int getCount()
    { return _count;
    }

    private int _count=0;
    private FieldInfo[] _schema=null;
  }


	class SqlDataHandler
		implements DataHandler
	{

    private void assertUpdateKey(FieldInfo[] fields)
    {
          Iterator it=_updateKey.iterator();
          while (it.hasNext())
          {
            String keyField=(String) it.next();
            boolean found=false;

            for (int i=0;i<fields.length;i++)
            {
              if (fields[i].getName().equals(keyField))
              { 
                found=true;
                break;
              }
            }
            if (!found)
            { 
              throw new RuntimeException
                ("Update key '"
                +keyField
                +"' not contained in "
                +ArrayUtil.format(fields,",",null)
                );
            }
          }
    }
		

    private int translateType(FieldInfo fieldInfo)
    {
      if (fieldInfo.getType()!=null)
      { 
        Integer sqlType=TypeMap.getSqlTypeFromSqlName(fieldInfo.getType());
        if (sqlType!=null)
        { return sqlType.intValue();
        }
        else
        { return TypeMap.getSqlTypeFromJavaType(fieldInfo.getType(),fieldInfo.getLength());
        }
      }
      else
      { return Types.VARCHAR;
      }
    }	
			
		public void handleFieldInfo(FieldInfo[] fields)
		{ 
      try
      {
        _connection
          =new DriverAgent
            (new ResourceConnectionInfo(_connectionResource))
              .connect();
        if (_connectionSetupSql!=null)
        { _connection.createStatement().execute(_connectionSetupSql);
        }
        
        if (_truncate)
        { _connection.createStatement().executeUpdate("TRUNCATE TABLE "+_tableName);
        }
            
        _connection.setAutoCommit(_autoCommit);

        _paramMap=new int[fields.length];
        _typeMap=new int[fields.length];
        

        if (_updateKey!=null && _checkKey)
        {
          assertUpdateKey(fields);
          //
          // Prepare a SELECT
          //
          StringBuffer sql=new StringBuffer();
          sql.append("SELECT count(*) FROM "+_tableName+" WHERE ");
          int count=0;
          int fieldCount=0;
          int keyCount=0;
          for (int i=0;i<fields.length;i++)
          { 
            FieldInfo fieldInfo=fields[i];
            _typeMap[count]=translateType(fieldInfo);
            String fieldName=fieldInfo.getName();
            int keyPos=_updateKey.indexOf(fieldName);

            if (keyPos>=0)
            {
              // Note the index of the key fields
              _paramMap[count]=(keyCount+1);
              if (keyCount>0)
              { sql.append(" AND ");
              }
              sql.append(fieldName);
              sql.append("= ?");
              keyCount++;
            }
            else
            { _paramMap[count]=-1;
            }
            count++;
          }

          _statement=_connection.prepareStatement(sql.toString());
          
        }
        else if (_updateKey!=null && !_checkKey)
        {
          assertUpdateKey(fields);

          //
          // Prepare an UPDATE
          //
          StringBuffer sql1=new StringBuffer();
          StringBuffer sql2=new StringBuffer();
          sql1.append("UPDATE "+_tableName+" SET ");
          sql2.append(" WHERE ");
          int count=0;
          int fieldCount=0;
          int keyCount=0;
          for (int i=0;i<fields.length;i++)
          { 
            FieldInfo fieldInfo=fields[i];
            _typeMap[count]=translateType(fieldInfo);
            String fieldName=fieldInfo.getName();
            int keyPos=_updateKey.indexOf(fieldName);

            if (keyPos>=0)
            {
              // Note the index of the key fields
              _paramMap[count]=( (_paramMap.length-_updateKey.size())+keyCount+1);
              if (keyCount>0)
              { sql2.append(" AND ");
              }
              sql2.append(fieldName);
              sql2.append("= ?");
              keyCount++;
            }
            else
            {
              // Create a 'SET' clause for non-key fields
              if (fieldCount>0)
              { sql1.append(",");
              }
              sql1.append(fieldName);
              sql1.append("= ? ");
              _paramMap[count]=fieldCount+1;
              fieldCount++;
            }
            count++;
          }

          _statement=_connection.prepareStatement(sql1.toString()+sql2.toString());
          _connection.commit();
        }
        else if (_delete)
        {
          //
          // Prepare a DELETE
          //
          StringBuffer sql1=new StringBuffer();
          StringBuffer sql2=new StringBuffer();
          sql1.append("DELETE FROM "+_tableName+" WHERE ");

          int count=0;
          for (int i=0;i<fields.length;i++)
          { 
            FieldInfo fieldInfo=fields[i];
            _typeMap[count]=translateType(fieldInfo);
            String fieldName=fieldInfo.getName();

            // Note the index of the key fields
            _paramMap[count]=count+1;
            if (count>0)
            { sql1.append(" AND ");
            }
            sql1.append(fieldName);
            sql1.append("= ?");
            count++;
          }

          _statement=_connection.prepareStatement(sql1.toString());
          
        }
        else
        {
          //
          // Prepare an INSERT
          //
          StringBuffer sql1=new StringBuffer();
          StringBuffer sql2=new StringBuffer();
          sql1.append("INSERT INTO "+_tableName+" (");
          sql2.append(" VALUES (");
    
          int count=0;
          for (int i=0;i<fields.length;i++)
          { 
            FieldInfo fieldInfo=fields[i];
            _typeMap[count]=translateType(fieldInfo);
            
            if (count>0)
            { 
              sql1.append(",");
              sql2.append(",");
            }
            sql1.append(fieldInfo.getName());
            sql2.append("?");
            _paramMap[count]=count+1;
            count++;
          }
          sql1.append(")");
          sql2.append(")");
          _statement=_connection.prepareStatement(sql1.toString()+sql2.toString());
        }

      }
      catch (SQLException x)
      { throw new RuntimeException(x.toString());
      }
      catch (IOException x)
      { throw new RuntimeException(x.toString());
      }
		}


   
		public void handleData(Object[] data)
		{
      boolean notAllNull=false;
      if (_insertKeyMapping!=null)
      {
        Object[] keyData=new Object[_insertKeyMapping.length];
        for (int i=0;i<_insertKeyMapping.length;i++)
        { keyData[i]=data[_insertKeyMapping[i]];
        }
        Key key=new Key(keyData);
        if (_keyMap.get(key)!=null)
        { 
          System.err.println("DUPLICATE KEY: "+ArrayUtil.format(data,",","\""));
          return;
        }
        else
        { _keyMap.put(key,key);
        }
      }
      
      try
      {
        if (_count>=_skipCount)
        {
          int fieldCount=0;
          for (int i=0;i<data.length;i++)
          { 
            Object dataObject=data[i];
            if (_paramMap[fieldCount]==-1)
            { 
              fieldCount++;
              continue;
            }
            if (dataObject!=null)
            { 
              try
              {
                switch(_typeMap[fieldCount])
                {
                case Types.TIMESTAMP:
                  _statement
                    .setObject
                      (_paramMap[fieldCount]
                      ,new Timestamp
                        (_dateFormat.parse((String) dataObject).getTime()
                        )
                      );
                  break;
                case Types.DOUBLE:
                  if (dataObject instanceof String)
                  { dataObject=Double.valueOf((String) dataObject);
                  }
                  _statement.setDouble(_paramMap[fieldCount],((Number) dataObject).doubleValue());
                  break;
                case Types.NUMERIC:
                  if (dataObject instanceof String)
                  { dataObject=new BigDecimal((String) dataObject);
                  }
                  _statement.setBigDecimal(_paramMap[fieldCount],new BigDecimal( ((Number) dataObject).doubleValue()));
                  break;
                case Types.INTEGER:
                  if (dataObject instanceof String)
                  { dataObject=Integer.valueOf((String) dataObject);
                  }
                  _statement.setInt(_paramMap[fieldCount],((Number) dataObject).intValue());
                  break;
                case Types.FLOAT:
                  if (dataObject instanceof String)
                  { dataObject=Float.valueOf((String) dataObject);
                  }
                  _statement.setFloat(_paramMap[fieldCount],((Float) dataObject).floatValue());
                  break;
                case Types.LONGVARCHAR:
                  _statement.setObject(_paramMap[fieldCount],dataObject,_typeMap[fieldCount]);
                  break;
                case Types.LONGVARBINARY:
                  _statement.setBytes(_paramMap[fieldCount],(byte[]) dataObject);
                  break;
                case Types.BIT:
                  if (dataObject instanceof Boolean)
                  { 
                    _statement.setBoolean
                      (_paramMap[fieldCount],((Boolean) dataObject).booleanValue());
                  }
                  break;
                default:
                  _statement.setObject(_paramMap[fieldCount],dataObject);
                }
              }
              catch (java.text.ParseException x)
              { throw new RuntimeException("Error parsing '"+dataObject+"' in "+data.toString()+":"+x.toString());
              }
              catch (ClassCastException x)
              { throw new RuntimeException("Unexpected data type '"+dataObject.getClass()+"' found for '"+dataObject+"' in "+data.toString());
              }
              notAllNull=true;
            }
            else
            { _statement.setNull(_paramMap[fieldCount],_typeMap[fieldCount]);
            }
            fieldCount++;
          }

          if (notAllNull)
          {
            if (!_checkKey)
            {
              int count=_statement.executeUpdate();
              if (count<1)
              { 
                if (_discardWriter!=null)
                { _discardWriter.println(data.toString());
                }
                else
                { _logWriter.println(data.toString());
                }
              }
              _count++;
              if (_count%_statusInterval==0)
              { _logWriter.println("Sent "+_count+" rows");
              }
              if (_count%_transactionSize==0)
              { 
                _connection.commit();
                _logWriter.println("Updated "+_count);
              }
            }
            else
            {
              ResultSet rs=_statement.executeQuery();
              if (!rs.next())
              { throw new SQLException("Null result!");
              }
              int keyCount=rs.getInt(1);
              if (keyCount<1)
              { _logWriter.println("Missing "+data);
              }
              else if (keyCount>1)
              { _logWriter.println("Duplicated "+keyCount+"x "+data); 
              }
              
              if (++_count%_transactionSize==0)
              { _logWriter.println("Queried "+_count);
              }
            }
          }
          else
          { _logWriter.println("Skipped null row "+_count);
          }
        }
        else
        { 
          if (++_count%_transactionSize==0)
          { _logWriter.println("Skipped "+_count);
          }
        }
      }
      catch (SQLException x)
      { 
        _logWriter.println
          ("Caught SqlException processing "
            +ArrayUtil.formatWithClassNames(data,",")
          );
        _logWriter.println(x.toString());
        throw new RuntimeException(x.toString());
      }

		}

	}
		


}

