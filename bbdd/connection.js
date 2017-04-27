'use strict';

/* Librerias */
var mysql       = require('mysql'),
    Q           = require('q'),
    logTime     = require('log4js'),
    bbdd        = require('./config.js');

/* Constructor */
function mysqlDB( config )
{
    /* Propiedades privadas */
    var _settings   = config ? config : bbdd.DB_CONFIG,
        pool        = mysql.createPool( _settings );
    /* Propiedades publicas */
    this.pool = pool;

    return this;

};
/* Ping */
mysqlDB.prototype.ping = function( err )
{
    if( err ) throw err;

    console.log('Server responded to ping');
};
/* Devuelve un array de conexiones */
mysqlDB.prototype.getConnections = function()
{
    return this.pool._freeConnections.length;
};

mysqlDB.prototype.query = function( queryString, dataQuery )
{
    var defer = Q.defer();

    this.pool.getConnection( function (err, connection) 
    {
        if (err) return defer.reject( err );

        var querySQL = connection.query( queryString, dataQuery, function(error, results, fields)
        {
            connection.release();

            //console.log( results );

            if (error) return defer.reject( error );

            return defer.resolve(results);
        });

        //console.log( querySQL.sql );
    }); 

    return defer.promise;
};

/* Finalizamos */
mysqlDB.prototype.end = function()
{
   this.pool.end();
}

module.exports = mysqlDB;