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
class DocTableSeedRx : BasePgRxVerticle() {

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
                        logger.info( "number of rows is ${count}")
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
                        logger.info( "row #${count}: id = ${row.getString( "id" ) }")
                        var tuple = Tuple.tuple()
                                .addInteger( row.getInteger( "id" ) )
                                .addString( row.getString( "name" ) )
                                .addString( row.getString( "short_description" ) )
                                .addString( row.getString( "author" ) )
                                .addString( row.getString( "description" ) )
                                .addString( row.getString( "content" ) )
                                .addValue( row.getValue( "last_updated" ) )
                                .addValue( row.getValue( "created" ) )
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
                                            logger.error("doc template seeding error while inserting", err)
                                            vertx.eventBus().send(WORK_ERRORED_ADDR,
                                                    "problems: ${this.javaClass.name} : ${err.message}")
                                        }
                                )
                    }

                    override fun onError(err: Throwable) {
                        logger.error( "doc template seeding error while reading", err )
                        vertx.eventBus().send( WORK_ERRORED_ADDR,
                                "problems: ${this.javaClass.name} : ${err.message}" )
                    }
                })

        startFuture.complete()
    }

}
