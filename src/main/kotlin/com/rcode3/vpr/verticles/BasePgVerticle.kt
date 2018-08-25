package com.rcode3.vpr.verticles

import io.reactiverse.pgclient.PgPool
import io.vertx.core.AbstractVerticle

abstract class BasePgVerticle : AbstractVerticle() {
    lateinit var srcDbPool  : PgPool
    lateinit var destDbPool : PgPool

    fun srcSchema() : String {
        return com.rcode3.vpr.utils.srcSchema( config() )
    }

    fun destSchema() : String {
        return com.rcode3.vpr.utils.destSchema( config() )
    }

    fun fetchSize() : Int {
        var retval : Int? = config().getInteger( "fetch_size" )
        if( retval == null ) {
            retval = 100
        }
        return retval
    }
}
