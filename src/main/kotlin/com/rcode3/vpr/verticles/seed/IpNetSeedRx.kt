package com.rcode3.vpr.verticles.seed

import io.reactiverse.reactivex.pgclient.PgRowSet
import io.reactiverse.reactivex.pgclient.Row
import io.reactiverse.reactivex.pgclient.Tuple
import io.vertx.core.Future
import mu.KLogging
import com.rcode3.vpr.verticles.BasePgRxVerticle
import com.rcode3.vpr.verticles.WORK_COMPLETED_ADDR
import com.rcode3.vpr.verticles.WORK_ERRORED_ADDR
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription


/**
 * Transfer table
 */
class IpNetSeedRx : BasePgRxVerticle() {

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

        srcRxDbPool().rxGetConnection()
                .flatMapPublisher { conn ->
                    conn.begin()
                    conn.rxPrepare( selectSql )
                            .flatMapPublisher { pq ->
                                val stream = pq.createStream( fetchSize(), Tuple.tuple() )
                                stream.toFlowable()
                            }
                            .doFinally {
                                conn.close()
                            }
                }
                .subscribe( object: Subscriber<Row>{

                    var writeCount = 0
                    lateinit var sub : Subscription
                    override fun onComplete() {
                        logger.info( "number of networks is ${count}")
                        vertx.eventBus().send( WORK_COMPLETED_ADDR, this.javaClass.name )
                    }

                    override fun onSubscribe(s: Subscription) {
                        sub = s
                        sub.request( fetchSize().toLong() )
                    }

                    override fun onNext(row: Row) {
                        count ++
                        if( count % fetchSize() == 0 ) {
                        //    logger.info( "row #${count}: net handle = ${row.getString( "net_handle" ) }")
                        }
                        logger.info( "row #${count}: net handle = ${row.getString( "net_handle" ) }")
                        var tuple = Tuple.tuple()
                                .addString( row.getString( "net_handle" ) )
                                .addString( row.getString( "org_handle" ) )
                                .addString( row.getString( "parent_net_handle" ) )
                                .addString( row.getString( "net_name" ) )
//                            .addValue( row.getValue( "reg_date" ) )
//                            .addString( row.getString( "pub_comment" ) )
//                            .addValue( row.getValue( "exp_date" ) )
//                            .addValue( row.getValue( "last_modified_date" ) )
//                            .addString( row.getString( "rsa_assignment_id" ) )
//                            .addValue( row.getValue( "revocation_date" ) )
//                            .addValue( row.getValue( "start_ip" ) )
//                            .addValue( row.getValue( "end_ip" ) )
//                            .addValue( row.getValue( "cidrs" ) )
//                            .addString( row.getString( "net_type" ) )
//                            .addValue( row.getValue( "origin_ases" ) )
                        destRxDbPool().rxPreparedQuery( insertSql, tuple )
                                .subscribe(
                                        { result : PgRowSet  ->
                                            logger.info( "write count: ${writeCount}")
                                            writeCount++
                                            if( writeCount >= fetchSize() ) {
                                                sub.request( fetchSize().toLong() )
                                                writeCount = 0
                                            }
                                        },
                                        { err : Throwable ->
                                            logger.error("Ip Net seeding error while inserting", err)
                                            vertx.eventBus().send(WORK_ERRORED_ADDR,
                                                    "problems: ${this.javaClass.name} : ${err.message}")
                                        }
                                )
                    }

                    override fun onError(err: Throwable) {
                        logger.error( "Ip Net seeding error while reading", err )
                        vertx.eventBus().send( WORK_ERRORED_ADDR,
                                "problems: ${this.javaClass.name} : ${err.message}" )
                    }
                })

        startFuture.complete()
    }

}
