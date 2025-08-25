package net.saddey.java_test_sqlserver_deadlock;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.springframework.util.function.ThrowingFunction;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main
{
  public static void main( String[] args ) throws SQLException, ExecutionException, InterruptedException, TimeoutException
  {
    DriverManager.registerDriver( new SQLServerDriver() );

    if( args.length != 3 )
    {
      System.err.println();
      System.err.println( "Usage: java " + getMyFileName() + " '<connection url>' '<user>' '<password>'" );
      System.err.println( "e.g. '<connection url>' = 'jdbc:sqlserver://;trustServerCertificate=true;serverName=sql.local.'" );
      System.err.println( "NB: Use single quotes (') for Linux / macos and double quotes (\") for Windows" );
      System.err.println();
      System.err.println(
          "This program uses / contains the Microsoft JDBC Driver for SQL Server whose license is available at\n"
              + "   https://github.com/microsoft/mssql-jdbc/blob/main/LICENSE" );
      System.err.println();
      return;
    }

    System.out.println();
    System.out.println( "Starting test... " + args[ 0 ] + " " + args[ 1 ] + " " + args[ 2 ] );

    new Deadlocking().createDeadlock( args[ 0 ], args[ 1 ], args[ 2 ] );

    System.out.println();

  }

  private static String getMyFileName()
  {
    try
    {
      return Optional
          .ofNullable( Main.class.getProtectionDomain() )
          .map( ProtectionDomain::getCodeSource )
          .map( CodeSource::getLocation )
          .map( ThrowingFunction.of( URL::toURI ) )
          .map( Paths::get )
          .map( Path::getFileName )
          .map( Path::toString )
          .filter( fn -> fn.endsWith( ".jar" ) )
          .map( fn -> "-jar " + fn )
          .orElseGet( Main.class::getSimpleName );
    }
    catch( RuntimeException e )
    {
      return Main.class.getSimpleName();
    }
  }

}