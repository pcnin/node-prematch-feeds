'use strict';

/* Libreria para la gestion de archivos */
const path = module.exports.path = {
    logs: '/var/www/html/cron/logs/',
    conf: '/var/www/html/cron/conf/',
    data: '/var/www/html/cron/conf_feeds_all/'
}

//var file      = require("fs");
var file      = require("graceful-fs");

/* Metodo para escribir los logs de datos para el administrador */
var write = module.exports.write =  function( path, text )
{
    try
    {
        var config = {
                encoding:'utf-8',
                flag:'a'
            }, 
            line = text + "\n";

        file.writeFile(path, line, config, function(error) 
        {
            if(error && (error.code === 'EMFILE' || error.code === 'ENFILE')) 
            {
                console.dir('[ERROR FILES WRITE]' + path + text + error);
                return;
            }
        });
    }
    catch( e )
    {
        console.dir( 'CATCH FILE WRITE: ' + e );
    } 
}

/* Metodo para abrir los logs de datos para el administrador */
var open = module.exports.open = function (path, mode)
{
    file.open(path, mode, function(error)
    {
        if(error && (error.code === 'EMFILE' || error.code === 'ENFILE'))
        {
            console.dir('[ERROR FILES OPEN]' + path + mode + error);
            return;
        } 
    });
}