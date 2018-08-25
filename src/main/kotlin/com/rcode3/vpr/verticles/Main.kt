package com.rcode3.vpr.verticles

import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.kotlin.core.DeploymentOptions
import mu.KLogging
import com.rcode3.vpr.DEST_DB_CONFIG
import com.rcode3.vpr.SRC_DB_CONFIG
import com.rcode3.vpr.verticles.seed.DocTableSeed

const val WORK_COMPLETED_ADDR = "work_completed.address"
const val WORK_ERRORED_ADDR   = "work_errored.address"

/**
 * Main verticle class that does basic checks and the starts the other
 * verticles.
 */
class Main : AbstractVerticle() {

    companion object : KLogging()

    override fun start(startFuture: Future<Void>) {

        logger.info( "Starting PG Replicator" )

        // prepare to listen for verticles that are done working.
        val eb = vertx.eventBus()
        eb.consumer<String>( WORK_COMPLETED_ADDR ) { message ->

            logger.info( "Stopping PG Replicator" )
            vertx.close()
        }
        eb.consumer<String>( WORK_ERRORED_ADDR ) { message ->
            logger.error( "Work errored out with: ${message.body()}")
            vertx.close()
        }

        val srcDb = PgInitVerify()
        srcDb.dbConfigName = SRC_DB_CONFIG
        val destDb = PgInitVerify()
        destDb.dbConfigName = DEST_DB_CONFIG

        val seedVerticles = listOf<BasePgVerticle>(
                DocTableSeed()
        )

        //replace this later with a real list
        val deltaVerticles = emptyList<BasePgVerticle>()

        //replace this later with logic to switch between the two.
        var verticleList = seedVerticles


        deployVerticle( srcDb )
        .compose{
            deployVerticle( destDb )
        }
        .compose{
            // pass the database pool to the verticles doing work
            seedVerticles.forEach {
                it.srcDbPool =  srcDb.pool
                it.destDbPool = destDb.pool
            }
            // now start the verticles doing the work
            CompositeFuture.all(
                    verticleList.map{ deployVerticle( it ) }
            )
        }.setHandler {
            if( it.succeeded() ) {
               startFuture.complete()
            }
            else {
                startFuture.fail( it.cause() )
            }
        }
    }

    /**
     * This creates a future and deploys the verticle, using [handleVerticleDeployment] as the handler.
     *
     * @return the future
     */
    fun deployVerticle( verticle: AbstractVerticle ) : Future<String> {
        val future = Future.future<String>()
        val options = DeploymentOptions().setConfig( config() )
        future.setHandler { handleVerticleDeployment( it ) }
        vertx.deployVerticle( verticle, options, future.completer() )
        return future
    }

    /**
     * Does something with the deployment of the verticle.
     */
    fun handleVerticleDeployment(result: AsyncResult<String>) {
        if( result.succeeded() ) {
            logger.debug{ "Deployment of ${result.result()} succeeded" }
        }
        else {
            logger.error( "Deployment of ${result.result()} failed", result.cause() )
        }
    }
}
