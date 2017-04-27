'use strict';

/**********************************************************/
/*                      Includes                          */
/**********************************************************/
var log4js  	= require('log4js'),
	Q  			= require('q'),
	_			= require('underscore'),
    utf8        = require('utf8'),
    dateformat  = require('dateformat'),
    markets     = require('./markets.js'),
    files       = require('./dataFile.js');

/**********************************************************/
/*                   Instanciación                        */
/**********************************************************/
var event = module.exports.event = function( data, dictionary, mysql, logger )
{
    try
    {
        /* Instanciamos constantes / variables principales */
        var defer           = Q.defer(),        // Promesa
            timeout         = 60000,            // Espacio de tiempo que se considera para realizar una acción (milisegundos)
            leagueData      = undefined,        // Datos de la liga para el evento que estamos tratando
            eventData       = undefined,        // Datos del evento que estamos tratando
            participantData = undefined,        // Datos de los participantes para el evento que estamos tratando
            marketsData     = undefined,        // Datos de los mercados para el evento que estamos tratando
            query           = undefined,        // Query para acceder a la base de datos
            updateHandle    = false,            // Para saber si tenemos que actualizar el handle
            reverse         = false;            // Variable para saber si le hemos dado la vuelta a los participantes

        /* Ficheros de log */
        var leaguesLog      = files.path.logs + "leagues.log",
            participantLog  = files.path.logs + "participants.log",
            eventLog        = files.path.logs + "event.log",
            marketLog       = files.path.logs + "market.log";

        // files.open(leaguesLog, 'a' );
        // files.open(participantLog, 'a' );
        // files.open(eventLog, 'a' );
        // files.open(marketLog, 'a' );

		/* Comenzamos a procesar el evento */
		/**********************************************************/
		/*** 	PASO 1. Buscamos la competición del evento 		***/
		/**********************************************************/
        var leagueData = _.find( dictionary.leagues, function( league )
        {
        	/* Buscar por string */
        	if( data.idLeague == 0 )
            	return league.str_league.toLowerCase() === data.nameLeague.toLowerCase();
            /* Buscar por id */
            else
            	return league.handle == data.idLeague; 
        });

        // /* CASO ESPECIAL DE GOLDENPARK */
        // /* Cambian de vez en cuanto los handle de las competiciones */
        // /* con lo cual debemos de modificarlo habitualmente */
        // if( leagueData && data.idFeed == 8 )
        // {
        //     mysql.query({
        //         sql: "UPDATE bg_league_feed SET handle = ? WHERE id_feed = ? AND str_league = ?",
        //         timeout: timeout,
        //         values : [data.idLeague, data.idFeed, utf8.encode(String(data.str_league)) ]
        //     })
        //     .then( function (res)
        //     {
        //         logger.warn( "[WARNING] Actualizada el handle de la competicion : " + data.str_league + " con handle ( " + data.idLeague + " )" );
        //     })
        //     .catch( function (err)
        //     {
        //         logger.error( "[ERROR] NO se ha actualizado el handle de la competicion : " + data.str_league + " con handle ( " + data.idLeague + " )" );
        //     });
        // }

        /* Si encontramos la competición */
        if( leagueData )
        {
            /**********************************************************/
            /***    PASO 2. Buscamos los participantes del evento   ***/
            /**********************************************************/
            /* Buscamos el caracter de corte del evento de entre todos los de la competicion */
            var explodeChar = _.find( leagueData.explode, function( chars )
            {
                return data.nameEvent.split(chars).length == 2;
            });

            if( explodeChar != undefined )
            {
                /* Hemos encontrado el caracter de corte */
                /* Se corta el evento para encontrar los participantes. Se eliminan los textos del tipo (Ganador playoff) del evento */
                var aParticipants   = data.nameEvent.split(explodeChar),
                    regExp          = "/\s\(.+\)*/",
                    aPartNumber     = [];
                aParticipants[0] = aParticipants[0].replace(regExp, "");
                aParticipants[1] = aParticipants[1].replace(regExp, "");

                /* Buscar los participantes en los diccionarios */
                var partHome = getParticipant( dictionary.participants, aParticipants[0] ),
                    partAway = getParticipant( dictionary.participants, aParticipants[1] );

                /* Si no tenemos los 2 participantes */
                if( partHome == undefined && partAway == undefined )
                {
                    /* Buscamos parejas de participantes (DOUBLES) */
                    var dParticipantHome = aParticipants[0].split("/");
                    if( dParticipantHome.length == 2 )
                    {
                        /* Buscar los participantes en los diccionarios */
                        var dHomeOne = getParticipant( dictionary.participants, dParticipantHome[0].trim() ),
                            dHomeTwo = getParticipant( dictionary.participants, dParticipantHome[1].trim() );

                        if( dHomeOne != undefined && dHomeTwo != undefined )
                        {
                            if( dHomeOne != undefined )
                                aPartNumber.push( dHomeOne.id_participant );
                            if( dHomeTwo != undefined )
                                aPartNumber.push( dHomeTwo.id_participant );

                             partHome = [dHomeOne, dHomeTwo];
                        }

                    }
                    var dParticipantAway = aParticipants[1].split("/");
                    if( dParticipantAway.length == 2 )
                    {
                        /* Buscar los participantes en los diccionarios */
                        var dAwayOne = getParticipant( dictionary.participants, dParticipantAway[0].trim() ),
                            dAwayTwo = getParticipant( dictionary.participants, dParticipantAway[1].trim() );

                        if( dAwayOne != undefined && dAwayTwo != undefined )
                        {
                            if( dAwayOne != undefined )
                                aPartNumber.push( dAwayOne.id_participant );
                            if( dAwayTwo != undefined )
                                aPartNumber.push( dAwayTwo.id_participant );

                            partAway = [ dAwayOne, dAwayTwo ];
                        }
                    }
                }
                else
                {
                    if( partHome != undefined ) aPartNumber.push( partHome.id_participant );
                    if( partAway != undefined ) aPartNumber.push( partAway.id_participant );
                }

                /* Tenemos los 2 participantes o nos falta uno de ellos */
                participantData = {
                    home: partHome ,
                    away: partAway
                };

                /*****************************************************************************/
                /* Caso especial de baloncesto que hay que dar la vuelta a los participantes */
                if( explodeChar == " @ " || explodeChar == ' en casa de ' )
                {
                    var auxHome = participantData.home,
                        auxAway = participantData.away

                    participantData = {
                        home: auxAway,
                        away: auxHome
                    }

                    logger.warn( "[Warning] Encontrado caracter de corte en el que cambiamos el orden de participantes: " + data.nameEvent );
                    reverse = true;
                }
            }
            else
            {
                /* Puede ser debido a que sea una competición que hay que crear de forma manual */
                /* como ganador de competición, maximo goleador, etc... */
                logger.warn( "[WARNING] No hemos podido cortar el evento: " + data.nameEvent );
            }

            /**********************************************************/
            /***            PASO 3. Buscamos el evento              ***/
            /**********************************************************/
            var eventData = _.find( dictionary.events, function( event )
            {
                /* Buscamos por el identificador del evento (handle) */
                if( event.handle != 0 && event.handle == data.idEvent )
                {
                    return true;
                }
                /* Buscamos por los participantes */
                else if( event.a_participant.length > 0 )
                {
                    var diff = _.difference( event.a_participant, aPartNumber );

                    /* Si no hay diferencias de participantes */
                    //if( diff.length == 0 )
                    if( diff.length == 0 && dateformat(event.date_event, "yyyy-mm-dd HH:MM:ss") == data.eventDate )
                    {
                        console.log("-----------------------------");
                        console.log( "Fecha: %s", dateformat(event.date_event, "yyyy-mm-dd HH:MM:ss") == data.eventDate);
                        console.log( dateformat(event.date_event, "yyyy-mm-dd HH:MM:ss") + " == " + data.eventDate);
                        console.log( "Handle: %s", event.handle == data.idEvent );
                        console.log(event.handle + " == " + data.idEvent);
                        console.log("-----------------------------");

                        /* Si el handle viene cambiado, pero es la misma fecha modificamos */
                        /* el handle cuando vayamos a modificar los datos del evento */
                        if( event.handle != data.idEvent ) updateHandle = true;

                        /* Vamos a intentar encontrar los participantes cambiados */
                        switch( event.a_participant.length )
                        {
                            case 2:
                                /* Si los participantes estan cambiados [1,2] == [2,1] */
                                if( arrayEq( event.a_participant, aPartNumber.reverse() ) )
                                {
                                    logger.warn( "[Warning] Los participantes vienen cambiados en: " + data.nameEvent );
                                    reverse = true;
                                }
                            break;
                            case 4:
                                /* Participantes dobles estan cambiados [1,2/3,4] == [3,4/1,2] || [1,2/3,4] == [3,4/2,1] */
                                if( arrayEq( _.initial( event.a_participant, 2 ), _.rest( aPartNumber, 2) ) || 
                                    arrayEq( _.initial( event.a_participant, 2 ), _.rest( aPartNumber, 2).reverse() ) )
                                {
                                    logger.warn( "[Warning] Los participantes vienen cambiados en: " + data.nameEvent );
                                    reverse = true;
                                }

                            break;
                        }
                        return true;
                    }
                    
                }
                /* Buscamos por el nombre del evento */
                else if( event.str_event == data.nameEvent )
                {
                    return true;
                }
            });

            /* Si hemos encontado el evento */
            if( eventData != undefined )
            {
                /* Actualizamos la información del evento */
                updateEvent()
                .then(function (res)
                {
                    /**********************************************************/
                    /***        PASO 4. Procesamos los mercados             ***/
                    /**********************************************************/
                    processMarkets()
                    .then(function(res)
                    {
                        return defer.resolve( getEndData() );
                    })
                    .catch( function (err)
                    {
                        logger.error( "[ERROR] Ocurrio un error a la hora procesar los mercados del evento: %s", data.nameEvent );
                        logger.error( err );
                        return defer.reject( getEndData() );
                    });
                })
                .catch( function (err)
                {
                    logger.error( "[ERROR] Ocurrio un error a la hora de actualizar el evento: %s", data.nameEvent  );
                    logger.error( err );
                    return defer.reject( getEndData() );
                });
            }
            /* Si no encontramos el evento */
            else
            {
                /* Si hemos encontrado todos los participantes */
                if( participantData != undefined && participantData.home != undefined && participantData.away != undefined )
                {
                    /* Creamos el evento */
                    insertEvent()
                    .then(function (res)
                    {
                        /**********************************************************/
                        /***        PASO 4. Procesamos los mercados             ***/
                        /**********************************************************/
                        processMarkets()
                        .then(function(res)
                        {
                            return defer.resolve( getEndData() );
                        })
                        .catch( function (err)
                        {
                            logger.error( "[ERROR] Ocurrio un error a la hora procesar los mercados del evento: %s", data.nameEvent );
                            logger.error( err );
                            return defer.reject( getEndData() );
                        });
                    })
                    .catch( function (err)
                    {
                        logger.error( "[ERROR] Ocurrio un error a la hora de insertar el evento: %s", data.nameEvent );
                        logger.error( err );
                        return defer.reject( getEndData() );
                    });
                }
                else
                {
                    // console.log( partHome );
                    // console.log( dParticipantHome );
                    // console.log( dHomeOne );
                    // console.log( dHomeTwo );
                    // console.log( "" );
                    // console.log( partAway );
                    // console.log( dParticipantAway );
                    // console.log( dAwayOne );
                    // console.log( dAwayTwo );
                    // console.log( "" );
                    // console.log( data.nameEvent );

                    if( aParticipants )
                    {
                        if( partHome == undefined && (dParticipantHome == undefined || dParticipantHome.length != 2 ) )
                        {
                            logger.info( "[INFO] No se encuentra el participante: " + aParticipants[0] );
                            files.write( participantLog, utf8.decode(String(data.idSport + "|" + data.idFeed + "|" + aParticipants[0].trim())) );
                            
                        }
                        else if( dParticipantHome != undefined && dParticipantHome.length == 2 )
                        {
                            if( dHomeOne == undefined )
                            {
                                logger.info( "[INFO] No se encuentra el participante: " + dParticipantHome[0] );
                                files.write( participantLog, utf8.decode(String(data.idSport + "|" + data.idFeed + "|" + dParticipantHome[0].trim())) );
                            }
                            if( dHomeTwo == undefined )
                            {
                                logger.info( "[INFO] No se encuentra el participante: " + dParticipantHome[1] );
                                files.write( participantLog, utf8.decode(String(data.idSport + "|" + data.idFeed + "|" + dParticipantHome[1].trim())) );
                            }
                        }
                        

                        if( partAway == undefined && ( dParticipantAway == undefined || dParticipantAway.length != 2 ) )
                        {
                            logger.info( "[INFO] No se encuentra el participante: " + aParticipants[1] );
                            files.write( participantLog, utf8.decode(String(data.idSport + "|" + data.idFeed + "|" + aParticipants[1].trim())) );
                        }
                        else if( dParticipantAway != undefined && dParticipantAway.length == 2 )
                        {
                            if( dAwayOne == undefined )
                            {
                                logger.info( "[INFO] No se encuentra el participante: " + dParticipantAway[0] );
                                files.write( participantLog, utf8.decode(String(data.idSport + "|" + data.idFeed + "|" + dParticipantAway[0].trim())) );
                            }
                            if( dAwayTwo == undefined )
                            {
                                logger.info( "[INFO] No se encuentra el participante: " + dParticipantAway[1] );
                                files.write( participantLog, utf8.decode(String(data.idSport + "|" + data.idFeed + "|" + dParticipantAway[1].trim())) );
                            }
                        }   
                    }

                    logger.warn( "[WARNING] No se encuentra el evento: " + data.nameEvent );
                    files.write( eventLog, utf8.decode(String(data.idFeed + "|" + data.idSport + "|" + data.idLeague + "|" + data.nameLeague + "|" + data.eventDate + "|" + data.idEvent + "|" + data.nameEvent)) );
                    return defer.resolve( getEndData() );
                }
            }
        }
        else
        {
            logger.warn( "[WARNING] No se encuentra la competicion: " + data.nameLeague );
            files.write( leaguesLog, utf8.decode(String(data.idFeed + "|" + data.idSport + "|" + data.idLeague + "|" + data.nameLeague)) );

            return defer.resolve( getEndData() );
        }
        /*************************/
        /* Al final debemos devolver, datos de LIGA - PARTICIPANTE - MERCADOS == Dictionary
        /*************************/
        return defer.promise;
    }
    catch( e )
    {
        logger.fatal( e );
        return defer.reject();
    }

    /**********************************************************/
    /*                      Funciones                         */
    /**********************************************************/
    /* Función para devolver los datos tratados de la feed */
    function getEndData()
    {
        return {
            league: leagueData,
            event: eventData,
            participants: participantData,
            markets: marketsData
        }
    };
    /* Función para saber si los participantes son los mismos */
    function arrayEq( a, b )
    {
        return _.all(_.zip(a, b), function(x) 
        {
            return x[0] === x[1];
        });
    };
    /* Buscamos un participante */
    function getParticipant( aParticipants, find )
    {
        return _.find(aParticipants, function( participant )
        {
            //console.log( participant.participant_feed + " == " + find.trim() );
            return participant.participant_feed == find.trim();
        });
    };
    /* Actualizamos la información del evento */
    function updateEvent() 
    {
        var deferer = Q.defer();

        /* Actualizamos los datos del evento de la feed */
        if( updateHandle )
        {
            /* Actualizamos el handle, por ser antiguo */
            query = {
                sql: "UPDATE bg_event_feed SET date_update_event = NOW(), handle = ? WHERE id_event_feed = ?",
                timeout: timeout,
                values : [  data.idEvent, eventData.id_event_feed  ]
            };

            logger.debug( "[DEBUG] Actualizamos el handle y el evento: " + data.nameEvent );
        }
        else
        {
            query = {
                sql: "UPDATE bg_event_feed SET date_update_event = NOW() WHERE id_event_feed = ?",
                timeout: timeout,
                values : [ eventData.id_event_feed ]
            };

            logger.debug( "[DEBUG] Actualizamos el evento: " + data.nameEvent );
        }

        /* Actualizamos la fecha de actualización del evento de la feed */
        mysql.query( query )
        .then( function (res)
        {
            /* Actualizamos la fecha del evento */
            switch(data.idFeed)
            {
                /* PAF con fecha en UTC */
                //case 2:
                /* TITANBET con fecha en UTC */
                case 13:
                /* MARCA con fecha en UTC */
                case 6:
                /* LUCKIA con fecha en UTC */
                case 17:
                    /* Comprobamos que no sea un evento manual (ganador competición, maximo goleador, etc..) */
                    if( eventData.a_participant.length > 0 )
                    {
                        query = {
                            sql: "UPDATE bg_event SET date_event = ?, date_update_event = NOW() WHERE id_event = ?",
                            timeout: timeout,
                            values : [  data.eventDate , eventData.id_event  ]
                        };
                    }
                    else
                    {
                        query = {
                            sql: "UPDATE bg_event SET date_update_event = NOW() WHERE id_event = ?",
                            timeout: timeout,
                            values : [  eventData.id_event  ]
                        };
                    }
                break;
                default:
                    query = {
                        sql: "UPDATE bg_event SET date_update_event = NOW() WHERE id_event = ?",
                        timeout: timeout,
                        values : [  eventData.id_event  ]
                    };
                break;
            }

            /* Actualizamos la fecha de actualización del evento */
            mysql.query(query)
            .then( function (res)
            {
                return deferer.resolve();
            })
            .catch( function (err)
            {
                logger.error( "[ERROR] Ha ocurrido un error en la query de actualización del evento" );
                logger.error( err );

                return deferer.reject();
            });
        })
        .catch( function (err)
        {
            logger.error( "[ERROR] Ha ocurrido un error en la query de actualización del evento" );
            logger.error( err );

            return deferer.reject();
        });

        return deferer.promise;
    };

    /* Insertamos un nuevo evento */
    function insertEvent()
    {
        var deferer = Q.defer(),
            strNameEvent = Object.prototype.toString.call( participantData.home ) == '[object Array]' ? 
                        participantData.home[0].participant + "/" + participantData.home[1].participant + " vs " + participantData.away[0].participant + "/" + participantData.away[1].participant :
                        participantData.home.participant + " vs " + participantData.away.participant;

        // INSERT INTO bg_event ( id_league, str_event, date_event, date_update_event, date_created, a_participant ) 
        // SELECT ?, ?, ?, NOW(), NOW(), ? FROM dual 
        // WHERE NOT EXISTS (
        //     SELECT str_event FROM bg_event WHERE id_league = ? AND str_event =? AND date_event > NOW() )


        // INSERT INTO bg_event ( id_league, str_event, date_event, date_update_event, date_created, a_participant )
        // SELECT * FROM ( SELECT ?, ?, ?, NOW(), NOW(), ? ) AS tmp
        // WHERE NOT EXISTS (
        //     SELECT str_event FROM bg_event WHERE id_league = ? AND str_event =? AND date_event > NOW()
        // ) LIMIT 1;


        mysql.query({
            //sql: "INSERT INTO bg_event ( id_league, str_event, date_event, date_update_event, date_created, a_participant ) SELECT ?, ?, ?, NOW(), NOW(), ? FROM dual WHERE NOT EXISTS (SELECT str_event FROM bg_event WHERE id_league = ? AND str_event =? AND date_event > NOW() )",
            sql: "INSERT INTO bg_event ( id_league, str_event, date_event, date_update_event, date_created, a_participant ) SELECT * FROM ( SELECT ?, ?, ?, NOW() AS str_update_event, NOW() AS str_created, ? ) AS tmp WHERE NOT EXISTS (SELECT str_event FROM bg_event WHERE id_league = ? AND str_event =? AND date_event > NOW() ) LIMIT 1",
            timeout: timeout,
            values : [ leagueData.id_league, strNameEvent, data.eventDate, "["+aPartNumber.toString()+"]", leagueData.id_league, strNameEvent ]
        })
        .then( function (res)
        {
            if( res.insertId == 0 )
            {
                logger.info( "[INFO] El evento ya existe: " + utf8.decode(String(strNameEvent)) );

                /* Buscamos el evento */
                mysql.query({
                    sql: "SELECT id_event, str_event FROM bg_event WHERE id_league = ? AND str_event = ? AND date_event > NOW()",
                    timeout: timeout,
                    values : [ leagueData.id_league, strNameEvent ]
                })
                .then( function (res)
                {
                    // var query = {
                    //     sql: "INSERT INTO bg_event_feed ( id_league, id_event, id_feed, str_event, date_event, date_update_event,link_event, handle ) SELECT ?, ?, ?, ?, ?, NOW(), ?, ?  FROM dual WHERE NOT EXISTS (SELECT id_event_feed FROM bg_event_feed WHERE id_event = ? AND id_league = ? AND id_feed = ? AND date_event > NOW())",
                    //     timeout: timeout,
                    //     values : [ leagueData.id_league, res[0].id_event, data.idFeed, utf8.encode(String(data.nameEvent)), data.eventDate, data.eventLink, data.idEvent, res[0].id_event, leagueData.id_league, data.idFeed ]
                    // };

                    /* Insertamos el nuevo evento de la feed */
                    mysql.query({
                        sql: "INSERT INTO bg_event_feed ( id_league, id_event, id_feed, str_event, date_event, date_update_event,link_event, handle ) VALUES ( ?, ?, ?, ?, ?, NOW(), ?, ? )",
                        timeout: timeout,
                        values : [ leagueData.id_league, res[0].id_event, data.idFeed, data.nameEvent, data.eventDate, data.eventLink, data.idEvent ]
                    })
                    .then( function (res_feed)
                    {
                        logger.debug( "[DEBUG] Insertamos un nuevo evento de la feed: " + data.nameEvent );

                        eventData = {
                            id_event: res[0].id_event,
                            id_event_feed: res_feed.insertId
                        };
                        return deferer.resolve();
                    })
                    .catch( function (err)
                    {
                        logger.error( "[ERROR] se ha producido un error al crear el evento (el evento existe): " + strNameEvent + " con handle ( " + data.idLeague + " )" );
                        return deferer.reject( err );
                    });
                })
                .catch( function (err)
                {
                    logger.error( "[ERROR] se ha producido un error al buscar el evento : " + strNameEvent + " con handle ( " + data.idLeague + " )" );
                    return deferer.reject( err );
                });
            }
            else
            {
                logger.debug( "[DEBUG] Insertamos un nuevo evento: " + data.nameEvent );
                console.log("Indentificador del evento creado: " + res.insertId );

                mysql.query({
                    sql: "INSERT INTO bg_event_feed ( id_league, id_event, id_feed, str_event, date_event, date_update_event,link_event, handle ) SELECT ?, ?, ?, ?, ?, NOW(), ?, ?  FROM dual WHERE NOT EXISTS (SELECT id_event_feed FROM bg_event_feed WHERE id_event = ? AND id_league = ? AND id_feed = ? AND date_event > NOW())",
                    timeout: timeout,
                    values : [ leagueData.id_league, res.insertId, data.idFeed, data.nameEvent, data.eventDate, data.eventLink, data.idEvent, res.insertId, leagueData.id_league, data.idFeed ]
                })
                .then( function (res_feed)
                {
                    logger.debug( "[DEBUG] Insertamos un nuevo evento de la feed: " + data.nameEvent );

                    eventData = {
                        id_event: res.insertId,
                        id_event_feed: res_feed.insertId
                    };

                    return deferer.resolve();
                })
                .catch( function (err)
                {
                    logger.error( "[ERROR] se ha producido un error al crear el evento (insertamos nuevo): " + strNameEvent + " con handle ( " + data.idLeague + " )" );
                    return deferer.reject( err );
                });                
            }
        })
        .catch( function (err)
        {
            logger.error( "[ERROR] se ha producido un error al crear el evento (primer insert) : " + strNameEvent + " con handle ( " + data.idLeague + " )" );
            return deferer.reject( err );
        });

        return deferer.promise;
    };

    /* Procesamos los mercados del evento */
    function processMarkets()
    {
        try
        {
            var deferer = Q.defer(),
                promiseMarket = [];

            logger.debug( "[DEBUG] Procesando mercados de: " + data.nameEvent );

            /* Recorremos todos los mercados */
            _.each( data.bets, function (bet)
            {
                /* Buscamos el mercado */
                var aMarsult = [],
                    fMarket = _.find(dictionary.markets, function( market )
                    {
                        return market.str_in_market == bet.name;
                    });

                /* Si hemos encontrado el mercado */
                if( fMarket != undefined )
                {
                    /* Hacemos el mercado */
                    switch( fMarket.id_group_market )
                    {
                        /* GRUPO 1 x 2 */
                        case 1:
                            switch( fMarket.id_market )
                            {
                                case 17:
                                    /* Doble Oportunidad */
                                    logger.info( "Doble Oportunidad" );
                                    aMarsult = markets.mDoubleChance( data, bet.bets, participantData, dictionary.participants, fMarket, reverse, logger );
                                break;
                                case 20:
                                    /* Descanso/Final */
                                    logger.info( "Descanso/Final" );
                                    aMarsult = markets.mHalfFull( data, bet.bets, participantData, dictionary.participants, fMarket, reverse, logger );
                                break;
                                default:
                                    /* 1 X 2 */
                                    logger.info( "1 X 2" );
                                    aMarsult = markets.m1X2( data.nameEvent, bet.bets, participantData, dictionary.participants, fMarket, reverse, logger );
                                break;
                            }
                        break;
                        case 2:
                            /* GRUPO Marcador correcto y handicap */
                            switch( fMarket.id_market)
                            {
                                /* Handicap */
                                case 9:
                                    logger.info( "European Handicap" );
                                    aMarsult = markets.mEuropeanHandicap( data, bet.bets, participantData, dictionary.participants, fMarket, reverse, logger );
                                break;
                                default:
                                    /* Correct score */
                                    logger.info( "Marcador correcto" );
                                    aMarsult = markets.mCorrectScore( data, bet.bets, participantData, dictionary.participants, fMarket, reverse, logger );
                                break;
                            }
                        break;
                        /* GRUPO 1 X 2 (Ganador del partido con local/visitante) */
                        case 3:
                            logger.info( "Ganador del partido con local/visitante" );
                            aMarsult = markets.m1X2( data.nameEvent, bet.bets, participantData, dictionary.participants, fMarket, reverse, logger );
                        break;
                        /* GRUPO 4 Ganador de la competición */
                        case 4:
                            logger.info( "Ganador de la competición" );
                            aMarsult = markets.mLoadWinner( data, bet.bets, dictionary.participants, logger );
                         break;
                        /* GRUPO True/False */
                        case 5:
                            logger.info( "True/False" );
                            aMarsult = markets.mTrueFalse( data, bet.bets, logger );
                        break;
                        /* GRUPO Goles Under/Over y Handicap de basket */
                        case 6:
                            switch( fMarket.id_market)
                            {
                                case 16:
                                     /* Handicap Baloncesto */
                                     logger.info( "European Handicap Basket" );
                                     aMarsult = markets.mEuropeanHandicapBasket( data, bet.bets, participantData, dictionary.participants, fMarket, reverse, logger );
                                break;
                                default:
                                    /* Goles Under/Over  */
                                    logger.info( "Goles Under/Over" );
                                    aMarsult = markets.mGoalsOverUnder( data, bet.bets, logger );
                                break;
                            }
                        break;
                    }

                    /* Hay datos para el mercado . Se almacena la promesa que graba el mercado */
                    if( aMarsult.length > 0 )
                        promiseMarket.push( recordMarket( aMarsult, fMarket ) );
                }
                else
                {
                    logger.warn( "[Warning] No se encontro el mercado: " + bet.name + " para el evento: " + data.nameEvent + " en la competicion: " + data.nameLeague );
                    files.write( participantLog, utf8.decode(String( data.idFeed + "|" + data.idSport + "|" + data.nameLeague + "|" + data.nameEvent + "|" + bet.name )) );
                }
            });

            /* Cuando todas las promesas de los mercados han acabado */
            Q.allSettled(promiseMarket)
            .timeout( timeout )
            .then( function ( res )
            {
                return deferer.resolve( res );
            })
            .catch(function (err)
            {
                return deferer.reject( err );
            });

            return deferer.promise;
        }
        catch( err )
        {
            console.log( err );
        };
    };

    /* Procesamos los mercados */
    function recordMarket( aMarsult, fMarket )
    {
        var deferer = Q.defer();

        /* Buscamos datos del mercado para saber si tenemos que insertalo o actualizarlo */
        mysql.query({
            sql: "SELECT id_market_feed FROM bg_market_feed WHERE id_market = ? AND id_event = ? AND id_event_feed = ?",
            timeout: timeout,
            values : [ fMarket.id_market, eventData.id_event, eventData.id_event_feed ]
        })
        .then( function (res)
        {
            /* Si ya existe el mercado */
            if( res.length > 0 )
                /* Updateamos sus cuotas */
                return deferer.resolve( recordOdd( res[0].id_market_feed, aMarsult, fMarket ) );
            /* Si el mercado todavia no existe */
            else
            {
                /* Creamos el mercado */
                mysql.query({
                    sql: "INSERT INTO bg_market_feed ( id_market, market_id_feed, id_event, id_event_feed, str_market ) VALUES (?, ?, ?, ?, ?)",
                    timeout: timeout,
                    values : [ fMarket.id_market, 0, eventData.id_event, eventData.id_event_feed, fMarket.str_out_market ]
                })
                .then( function (result)
                {
                    return deferer.resolve( recordOdd( result.insertId, aMarsult, fMarket ) );
                })
                .catch( function (error)
                {
                    logger.error( "[ERROR] Al insertar un nuevo mercado." );
                    return deferer.reject( err );
                });
            }   
        })
        .catch( function (err)
        {
            logger.error( "[ERROR] Al buscar el mercado para insertar o actualizarlo." );
            logger.error( err );
            return deferer.reject( err );
        });

        return deferer.promise;
    };

    /* Procesamos las cuotas */
    function recordOdd( id_market_feed, aMarsult, fMarket )
    {
        var deferer = Q.defer(),
            oddPromises = [];

        _.each( aMarsult, function (dataBet)
        {
            oddPromises.push( itemOdd(dataBet, id_market_feed, aMarsult, fMarket) );
        });

        /* Cuando todas las promesas de los mercados han acabado */
        Q.allSettled(oddPromises)
        .timeout( timeout )
        .then( function ( res )
        {
            //console.log( res );
            //logger.warn("DEVOLVEMOS...");
            return deferer.resolve();
        })
        .catch(function (err)
        {
            logger.error( "[ERROR] DATOS INDEFINIDOS : '" + dataBet.name + " ("+ new_odd +")' , del mercado: " + fMarket.str_out_market + ", del evento: " + data.nameEvent + ", para la competición: " + data.nameLeague );
            logger.error( err );
            return deferer.reject();
        });

        return deferer.promise;
    };

    function itemOdd( bet, id_market_feed, aMarsult, fMarket )
    {
        var defererOdd = Q.defer();

        /* Ponemos el formato de la cueta */
        var new_odd = parseFloat(String(bet.odd).replace(",",".")).toFixed(2),
            query = "",
            itemQ = [];

        mysql.query({
            sql: "SELECT id_bet, n_odd FROM bg_bet_feed WHERE id_market_feed = ? AND id_feed = ? AND str_bet = ? LIMIT 1",
            timeout: timeout,
            values : [ id_market_feed, data.idFeed, bet.name ]
        })
        .then( function (res)
        {
            /* Si ya tenemos la apuesta */
            if( res.length > 0 )
            {
                var old_odd = parseFloat(res[0].n_odd).toFixed(2);

                 /* Si las cuotas son distintas hay que actualizar */
                if(new_odd != old_odd)
                {
                    query = "UPDATE bg_bet_feed SET n_odd = ?, n_odd_old = ?, date_update = NOW(), date_check = NOW(), str_link = ? WHERE id_bet = ?";
                    itemQ = [ new_odd, old_odd, bet.placebetlink, res[0].id_bet ];

                    logger.warn( "[WARNING] Modificamos la apuesta (HA CAMBIADO): '" + bet.name + " ("+ new_odd +")' , del mercado: " + fMarket.str_out_market + ", del evento: " + data.nameEvent + ", para la competición: " + data.nameLeague );
                }
                /* Si las cuotas son las mismas, solo actualizamos la fecha */
                else
                {
                    query = "UPDATE bg_bet_feed SET date_check = NOW(), str_link = ? WHERE id_bet = ?";
                    itemQ = [ bet.placebetlink, res[0].id_bet ];
                    
                    logger.warn( "[WARNING] Modificamos la apuesta (NO HA CAMBIADO): '" + bet.name + " ("+ new_odd +")' , del mercado: " + fMarket.str_out_market + ", del evento: " + data.nameEvent + ", para la competición: " + data.nameLeague );
                }

            }
            /* Insertamos la nueva apuesta */
            else
            {
                query = "INSERT INTO bg_bet_feed ( id_feed, id_market_feed, str_bet, str_bet_feed, n_odd, n_odd_old, date_update, date_check, str_link ) VALUES ( ?, ?, ?, ?, ?, ?, NOW(), NOW(), ? ) ";
                itemQ = [ data.idFeed, id_market_feed, bet.name, bet.name_feed, new_odd, new_odd, bet.placebetlink ];

                logger.warn( "[WARNING] Insertamos la apuesta (NUEVA): '" + bet.name + " ("+ new_odd +")' , del mercado: " + fMarket.str_out_market + ", del evento: " + data.nameEvent + ", para la competición: " + data.nameLeague );
            }

            /* Procesamos las cuotas */
            mysql.query({
                sql: query,
                timeout: timeout,
                values : itemQ
            })
            .then( function (result)
            {
                return defererOdd.resolve( result );
            })
            .catch( function (error)
            {
                logger.error( "[ERROR] En proceso de cuotas: '" + bet.name + " ("+ new_odd +")' , del mercado: " + fMarket.str_out_market + ", del evento: " + data.nameEvent + ", para la competición: " + data.nameLeague );
                logger.error( error );
                return defererOdd.reject( error );
            });

        })
        .catch( function (err)
        {
            logger.error( "[ERROR] Al procesar las cuotas del mercado." );
            logger.error( err );
            return defererOdd.reject( err );
        });

        return defererOdd.promise;
    };
};