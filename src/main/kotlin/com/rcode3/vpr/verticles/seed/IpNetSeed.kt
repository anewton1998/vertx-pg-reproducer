package com.rcode3.vpr.verticles.seed

import io.reactiverse.pgclient.PgConnection
import io.reactiverse.pgclient.Tuple
import io.vertx.core.Future
import mu.KLogging
import com.rcode3.vpr.verticles.BasePgVerticle
import com.rcode3.vpr.verticles.WORK_COMPLETED_ADDR
import com.rcode3.vpr.verticles.WORK_ERRORED_ADDR


class IpNetSeed : BasePgVerticle() {

    companion object : KLogging()

    override fun start(startFuture: Future<Void>) {

        logger.info( "IP Network Seeder starting." )

        val selectSql = """
            select
               net_address.net_handle,
               org_handle,
               parent_net_handle,
               net_name,
               reg_date,
               pub_comment,
               exp_date,
               last_modified_date,
               rsa_assignment_id,
               revocation_date,
               array(
                  select
                     net_address.start_ip || '/' || net_address.cidr_prefix
                  from
                     ${srcSchema()}.net_address
                  where
                     net_address.net_handle = net.net_handle
                  order by
                     net_address.start_ip
               ) as cidrs,
               array(
                  select
                     net_origin_as.origin_as_int::bigint
                  from
                     ${srcSchema()}.net_origin_as
                  where
                     net_origin_as.net_handle = net.net_handle
               ) as origin_ases
            from
               ${srcSchema()}.net_address, ${srcSchema()}.net
            where
               net_address.net_handle = net.net_handle
            group by
               net_address.net_handle, net.net_handle
        """.trimIndent()

        val insertSql = """
            insert into ${destSchema()}.ip_network
            (
               net_handle,
               org_handle,
               parent_net_handle,
               net_name
               -- reg_date,
               -- pub_comment,
               -- exp_date,
               -- last_modified_date,
               -- rsa_assignment_id,
               -- revocation_date,
               -- start_ip,
               -- end_ip,
               -- cidrs,
               -- net_type,
               -- origin_ases
            )
            values
            (
               $1, $2, $3, $4
               -- $5, $6, $7, $8,
               -- $9, $10, $11, $12, $13, $14, $15
            )
        """.trimIndent()

        var count = 0
        var writeCount = 0

        srcDbPool.getConnection { connHandler ->
            if( connHandler.succeeded() ) {
                var conn = connHandler.result()
                var tx = conn.begin().abortHandler {
                    logger.error("Ip Net seeding error transaction rollback", it)
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
                                    logger.error("Ip Net seeding error while streaming", it)
                                    vertx.eventBus().send(WORK_ERRORED_ADDR,
                                            "problems: ${this.javaClass.name} : ${it.message}")
                                }
                                .endHandler{
                                    tx.commit()
                                    conn.close()
                                    logger.info( "number of networks is ${count}")
                                    vertx.eventBus().send( WORK_COMPLETED_ADDR, this.javaClass.name )
                                }
                                .handler { row ->
                                    logger.info( "count is ${count} - net is ${row.getString( "net_handle" )}")
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
                                                    logger.error("Ip Net seeding error while inserting", rowSetHandler.cause())
                                                    vertx.eventBus().send(WORK_ERRORED_ADDR,
                                                            "problems: ${this.javaClass.name} : ${rowSetHandler.cause().message}")
                                                }
                                            }
                                        }
                                        else {
                                            tx.rollback()
                                            conn.close()
                                            logger.error("Ip Net seeding error while connecting to destination", destConnHandler.cause())
                                            vertx.eventBus().send(WORK_ERRORED_ADDR,
                                                    "problems: ${this.javaClass.name} : ${destConnHandler.cause().message}")
                                        }
                                    }
                                }
                    }
                    else {
                        tx.rollback()
                        conn.close()
                        logger.error("Ip Net seeding error while preparing", pqHandler.cause())
                        vertx.eventBus().send(WORK_ERRORED_ADDR,
                                "problems: ${this.javaClass.name} : ${pqHandler.cause().message}")
                    }
                }
            }
            else {
                logger.error("Ip Net seeding error while connecting", connHandler.cause())
                vertx.eventBus().send(WORK_ERRORED_ADDR,
                        "problems: ${this.javaClass.name} : ${connHandler.cause().message}")
            }
        }

        startFuture.complete()
    }

}
