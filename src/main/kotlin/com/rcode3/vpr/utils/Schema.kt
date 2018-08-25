package com.rcode3.vpr

import io.vertx.core.json.JsonObject
import com.rcode3.vpr.DEST_DB_CONFIG
import com.rcode3.vpr.SRC_DB_CONFIG

/**
 * Well known value for 'schema'
 */
const val SCHEMA_KW = "schema"

/**
 * Gets the schema of a database configuration. Defaults to "public" if none is specified.
 */
fun schema( config : JsonObject, db : String ) : String {
    var retval : String? = config.getJsonObject( db ).getString( SCHEMA_KW )
    if( retval == null ) {
        retval = "public"
    }
    return retval
}

/**
 * Gets the source database schema. Convenience method for [schema].
 */
fun srcSchema( config: JsonObject ) : String {
    return schema( config, SRC_DB_CONFIG )
}

/**
 * Gets the destination database schema. Convenience method for [schema].
 */
fun destSchema( config : JsonObject ) : String {
    return schema( config, DEST_DB_CONFIG )
}
