package net.saddey.java_test_sqlserver_deadlock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.springframework.util.function.ThrowingFunction;

public final class WithinSeparateThreadSqlExecutor implements AutoCloseable
{

  /** delegates to ConnectionThread instance in order to do ALL ops from same thread (though not required) */
  private Supplier<Connection> connectionSupplier;

  private final ExecutorService executorService;

  public WithinSeparateThreadSqlExecutor( final String connectionUrl, final String user, final String password )
  {
    executorService = Executors.newSingleThreadExecutor( r -> new ConnectionThread( connectionUrl, user, password, r ) );
  }

  private final class ConnectionThread extends Thread
  {

    ConnectionThread( final String url, final String user, final String password, final Runnable r )
    {
      super( r );
      try
      {
        final Connection connection = DriverManager.getConnection( url, user, password );
        connection.setAutoCommit( false );
        connectionSupplier = () -> connection;
      }
      catch( final SQLException e )
      {
        throw new RuntimeException( e );
      }
    }
  }

  @Override
  public void close()
  {
    executorService.shutdown();
    try
    {
      if( executorService.awaitTermination( 10, TimeUnit.SECONDS ) )
      {
        return; // all done as orderly shutdown complete
      }
    }
    catch( InterruptedException e )
    {
      // ignore
    }
    executorService.shutdownNow(); // sort of kill else

    final Optional<Connection> toBeClosedConnection = Optional.ofNullable( connectionSupplier ).map( Supplier::get );
    if( toBeClosedConnection.isPresent() )
    {
      try
      {
        toBeClosedConnection.get().close();
      }
      catch( Exception e )
      {
        System.err.println( "Error while closing connection: " + e );
      }
    }

  }

  public Future<Void> startToExecute( final String sql )
  {
    return startWithConnection( c -> {
      try (final Statement statement = c.createStatement())
      {
        //noinspection SqlSourceToSinkFlow
        statement.executeUpdate( sql );
      }
      return null;
    }, sql );
  }

  public Future<Void> startCommit()
  {
    return startWithConnection( c -> {
      c.commit();
      return null;
    }, "startCommit();" );
  }

  public <T> Future<T> startWithConnection( final ThrowingFunction<Connection, T> function, final String whatAreYouDoing )
  {
    return executorService.submit( () -> {
      try
      {
        return function.apply( connectionSupplier.get() );
      }
      catch( final RuntimeException e )
      {
        System.err.println( e + " While executing " + whatAreYouDoing );
        throw e;
      }
    } );
  }

}
