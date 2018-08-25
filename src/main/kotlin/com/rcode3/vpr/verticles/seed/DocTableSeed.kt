package com.rcode3.vpr.verticles.seed

import io.reactiverse.pgclient.Tuple
import io.vertx.core.Future
import mu.KLogging
import com.rcode3.vpr.verticles.BasePgVerticle
import com.rcode3.vpr.verticles.WORK_COMPLETED_ADDR
import com.rcode3.vpr.verticles.WORK_ERRORED_ADDR


class DocTableSeed : BasePgVerticle() {

    companion object : KLogging()

    override fun start(startFuture: Future<Void>) {

        logger.info( "doc template Seeder starting." )

        val selectSql = """
            select
               id,
               name,
               short_description,
               author,
               description,
               content,
               last_updated,
               created
            from
               ${srcSchema()}.document_template
        """.trimIndent()

        val insertSql = """
            insert into ${destSchema()}.document_template
            (
               id,
               name,
               short_description,
               author,
               description,
               content,
               last_updated,
               created
            )
            values
            (
               $1, $2, $3, $4,
               $5, $6, $7, $8
            )
        """.trimIndent()

        var count = 0
        var writeCount = 0

        srcDbPool.getConnection { connHandler ->
            if( connHandler.succeeded() ) {
                var conn = connHandler.result()
                var tx = conn.begin().abortHandler {
                    logger.error("doc template seeding error transaction rollback", it)
                    vertx.eventBus().send(WORK_ERRORED_ADDR,
                            "problems: ${this.javaClass.name} : ${it}")
                }
                conn.prepare( selectSql ) { pqHandler ->
                    if( pqHandler.succeeded() ) {
                        var pq = pqHandler.result()
                        var stream = pq.createStream( fetchSize(), Tuple.tuple() )
                        stream
                                .exceptionHandler {
                                    tx.rollback()
                                    conn.close()
                                    logger.error("doc template seeding error while streaming", it)
                                    vertx.eventBus().send(WORK_ERRORED_ADDR,
                                            "problems: ${this.javaClass.name} : ${it.message}")
                                }
                                .endHandler{
                                    tx.commit()
                                    conn.close()
                                    logger.info( "number of rows is ${count}")
                                    vertx.eventBus().send( WORK_COMPLETED_ADDR, this.javaClass.name )
                                }
                                .handler { row ->
                                    logger.info( "count is ${count} - id is ${row.getString( "ID" )}")
                                    count++
                                    destDbPool.getConnection { destConnHandler ->
                                        if( destConnHandler.succeeded() ) {
                                            var tuple = Tuple.tuple()
                                                    .addString( row.getString( "net_handle" ) )
                                                    .addString( row.getString( "org_handle" ) )
                                                    .addString( row.getString( "parent_net_handle" ) )
                                                    .addString( row.getString( "net_name" ) )
                                            var destConn = destConnHandler.result()
                                            destConn.preparedQuery( insertSql, tuple ) { rowSetHandler ->
                                                if( rowSetHandler.succeeded() ) {
                                                    logger.info( "write count: ${writeCount}")
                                                    writeCount++
                                                    destConn.close()
                                                    if( writeCount < fetchSize() ) {
                                                        logger.info( "pausing stream" )
                                                        stream.pause()
                                                    }
                                                    else {
                                                        logger.info( "resuming stream" )
                                                        stream.resume()
                                                        writeCount = 0
                                                    }
                                                }
                                                else {
                                                    tx.rollback()
                                                    conn.close()
                                                    logger.error("doc template seeding error while inserting", rowSetHandler.cause())
                                                    vertx.eventBus().send(WORK_ERRORED_ADDR,
                                                            "problems: ${this.javaClass.name} : ${rowSetHandler.cause().message}")
                                                }
                                            }
                                        }
                                        else {
                                            tx.rollback()
                                            conn.close()
                                            logger.error("doc template seeding error while connecting to destination", destConnHandler.cause())
                                            vertx.eventBus().send(WORK_ERRORED_ADDR,
                                                    "problems: ${this.javaClass.name} : ${destConnHandler.cause().message}")
                                        }
                                    }
                                }
                    }
                    else {
                        tx.rollback()
                        conn.close()
                        logger.error("doc template seeding error while preparing", pqHandler.cause())
                        vertx.eventBus().send(WORK_ERRORED_ADDR,
                                "problems: ${this.javaClass.name} : ${pqHandler.cause().message}")
                    }
                }
            }
            else {
                logger.error("doc template seeding error while connecting", connHandler.cause())
                vertx.eventBus().send(WORK_ERRORED_ADDR,
                        "problems: ${this.javaClass.name} : ${connHandler.cause().message}")
            }
        }

        startFuture.complete()
    }

}
