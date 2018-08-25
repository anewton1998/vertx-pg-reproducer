package com.rcode3.vpr.verticles

import io.reactiverse.reactivex.pgclient.PgPool

abstract class BasePgRxVerticle : BasePgVerticle() {

    fun srcRxDbPool(): PgPool {
        return PgPool( srcDbPool )
    }

    fun destRxDbPool() : PgPool {
        return PgPool( destDbPool )
    }

}
