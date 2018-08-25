package com.rcode3.vpr.verticles

import io.reactiverse.pgclient.PgClient
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.PgPoolOptions
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import mu.KLogging
import com.rcode3.vpr.utils.schema

/**
 * A verticle that creates a connection pool and verifies it.
 */
class PgInitVerify : AbstractVerticle() {

    var dbConfigName = "db"

    lateinit var pool : PgPool

    companion object : KLogging()

    override fun start(startFuture: Future<Void>) {

        // Get our specific database configuration options.
        var dbConfig : JsonObject?  = config().getJsonObject( dbConfigName )
        dbConfig?.let {

            var options = PgPoolOptions( dbConfig )

            // Create the client pool
            pool = PgClient.pool(vertx, options)

            // A simple query
            val sql = "select version from ${schema( config(), dbConfigName )}.flyway_schema_history where version = '${dbConfig.getString( "flywayVersion" )}'"
            pool.query( sql ) { ar ->
                if (ar.succeeded()) {
                    var result = ar.result()
                    logger.debug("Got ${result.size()} rows ")
                    if( result.size() == 0 ) {
                        startFuture.fail( "${dbConfigName} database does not seem to be setup" )
                    }
                    else {
                        logger.info( "$dbConfigName is initialized and verified." )
                        startFuture.complete()
                    }
                } else {
                    startFuture.fail( ar.cause() )
                }

            }

        } ?: startFuture.fail( "Cannot find database configuration for $dbConfigName" )

    }

}
