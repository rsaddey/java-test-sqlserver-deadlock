package net.saddey.java_test_sqlserver_deadlock;

import java.sql.ResultSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class Deadlocking
{
  void createDeadlock( final String connectionUrl, final String user, final String password )
      throws ExecutionException, InterruptedException, TimeoutException
  {

    try (final WithinSeparateThreadSqlExecutor exe1 = new WithinSeparateThreadSqlExecutor( connectionUrl, user, password );
        final WithinSeparateThreadSqlExecutor exe2 = new WithinSeparateThreadSqlExecutor( connectionUrl, user, password ))
    {
      // not required, just rule out accidental collateral damage
      exe1.startToExecute( "use tempdb;" );
      exe2.startToExecute( "use tempdb;" );

      exe1.startToExecute( "create table ##testDeadlock1 ( dummy int );" );
      exe1.startToExecute( "insert into ##testDeadlock1 (dummy) values (0);" );
      exe1.startToExecute( "create table ##testDeadlock2 ( dummy int );" );
      exe1.startToExecute( "insert into ##testDeadlock2 (dummy) values (0);" );
      exe1.startCommit().get( 30, TimeUnit.SECONDS );

      // .get -> insure updates done / locks held
      exe1.startToExecute( "update ##testDeadlock1 set dummy = 1;" ).get( 30, TimeUnit.SECONDS );
      exe2.startToExecute( "update ##testDeadlock2 set dummy = 2;" ).get( 30, TimeUnit.SECONDS );

      // deadlock: try to update / lock something already locked by other
      // random / arbitrary one chosen as victim by SQL Server
      exe1.startToExecute( "update ##testDeadlock2 set dummy = 1;" );
      exe2.startToExecute( "update ##testDeadlock1 set dummy = 2;" );

      // jdbc / AutoCommit are quite different from doing transactions in SQL
      // BOTH commits will succeed: the winner one and an empty tran for the looser
      exe1.startCommit().get( 30, TimeUnit.SECONDS );
      exe2.startCommit().get( 30, TimeUnit.SECONDS );
    }
    finally
    {
      try (final WithinSeparateThreadSqlExecutor cleanupExe = new WithinSeparateThreadSqlExecutor( connectionUrl, user,
          password ))
      {
        System.out.println( cleanupExe.startWithConnection( c -> {
          //noinspection SqlNoDataSourceInspection,SqlResolve
          try (final ResultSet resultSet = c.createStatement().executeQuery( //
              "select concat('And the winner is... " //
                  + "##testDeadlock1: ', (select top 1 dummy from ##testDeadlock1), " //
                  + "', ##testDeadlock2: ', (select top 1 dummy from ##testDeadlock2) );" ))
          {
            return resultSet.next() ? resultSet.getString( 1 ) : "no results";
          }
        }, "show results" ).get( 5, TimeUnit.SECONDS ) );

        // currently this is not required as ## objects wil vanish on session.close, but beware of connection pools...
        cleanupExe.startToExecute( "drop table if exists ##testDeadlock1;" );
        cleanupExe.startToExecute( "drop table if exists ##testDeadlock2;" );
        cleanupExe.startCommit().get( 30, TimeUnit.SECONDS );
      }
    }

  }

}
