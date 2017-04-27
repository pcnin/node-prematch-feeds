/**********************************************************/
/*                      Includes                          */
/**********************************************************/
var log4js  	= require('log4js'),
	parseString = require('xml2js').parseString,
	Q  			= require('q'),
	//request 	= require('request'),
	http        = require('http'),
	_			= require('underscore'),
	DB 			= require('./bbdd/connection.js'),
    record      = require("./record.js"),
	dictionary  = require('./dictionary.js'),
    dateUTC     = require('./dates.js'),
    utf8    	= require('utf8');

/**********************************************************/
/*                      Settings                          */
/**********************************************************/

const NAME_FEED = "MARCA",
	  ID_FEED 	= 6,
	  SPORTS 	= [{
	  		"id" 	: 1,
            "sport" : 'Football',
            "url" 	: 'http://genfeeds.marcaapuestas.es/odds_feed?key=get_events_for_sport&lang=es&sport_code=FOOT'
	  	},{
	  		"id" 	: 2,
            "sport" : 'Basketball',
            "url" 	: 'http://genfeeds.marcaapuestas.es/odds_feed?key=get_events_for_sport&lang=es&sport_code=BASK'
	  	},{
	  		"id" 	: 3,
            "sport" : 'Tennis',
            "url" 	: 'http://genfeeds.marcaapuestas.es/odds_feed?key=get_events_for_sport&lang=es&sport_code=TENN'
	  	},{
	  		"id" 	: 4,
            "sport" : 'Moto GP',
            "url" 	: 'http://genfeeds.marcaapuestas.es/odds_feed?key=get_events_for_sport&lang=es&sport_code=MOTO'
	  	},{
	  		"id" 	: 5,
            "sport" : 'Formula 1',
            "url" 	: 'http://genfeeds.marcaapuestas.es/odds_feed?key=get_events_for_sport&lang=es&sport_code=MOTO'
	  	},{
	  		"id" 	: 6,
            "sport" : 'Ciclismo',
            "url" 	: 'http://genfeeds.marcaapuestas.es/odds_feed?key=get_events_for_sport&lang=es&sport_code=CYCL'
	  	},{
            "id" 	: 7,
            "sport" : 'Boxeo',
            "url" 	: 'http://genfeeds.marcaapuestas.es/odds_feed?key=get_events_for_sport&lang=es&sport_code=BOXI'
	  	}];

log4js.configure({
  appenders: [
    { type: 'console' },
    //{ type: 'file', filename: 'logs/marca.log', category: 'marca' }
  ]
});

var logger 		 = log4js.getLogger('marca'),		
	mysql 		 = new DB(),
	listEvents 	 = [],
	promisesUrls = [],
    promises     = [];

try
{
	/* Iniciamos la carga del diccionario */
	logger.trace("Iniciamos la carga del diccionario" );
	dictionary.getDictionary( ID_FEED, mysql )
	.then(function( dictionaryData )
	{
		/* Se lee los datos de cada deporte */
        _.each( SPORTS, function( sportData )
        {
            /* Guardamos la promesa del deporte */
            promisesUrls.push( readSport(sportData, dictionaryData) ) ;
        });

        /* Cuando todas las promesas acaban, se cierra el pool y se liberan recursos */
        Q.allSettled(promisesUrls)
        //.timeout(60000)
        .then( function ( res )
        {
        	/* Terminando de recorrer los eventos */
            logger.trace("Terminamos de recorrer los eventos de la feed" );

            //console.log( JSON.stringify(listEvents) );

            /* Recorremos todos los eventos y vamos tramitandolos */
            _.map( listEvents, function( event )
            {
                /* Creamos el diccionario segun el evento.deporte */
                var dictionarySport = {
                    leagues: dictionaryData.leagues[ event.idSport ],
                    events: dictionaryData.events[ event.idSport ],
                    participants: dictionaryData.participants[ event.idSport ],
                    markets: dictionaryData.markets[ event.idSport ]
                }

                if( event.nameEvent == 'Osasuna vs Sporting' )
                	console.log( JSON.stringify(event) );

                //promises.push( record.event( event, dictionarySport, mysql, logger ) );
            });
	        /* Cuando todas las promesas acaban, se cierra el pool y se liberan recursos */
	        Q.allSettled(promises)
	        .timeout(60000)
	        .then( function ( res )
	        {
	        	console.log( res.length + " - "+  listEvents.length );
	            //console.log( JSON.stringify(res) );
	            //console.log( JSON.stringify(listEvents) );
	            //console.log("terminado>>>>>");

	            logger.info("Finalizada la carga total de la feed" );
	            mysql.end();
	        })
	        .catch(function (err)
	        {
	            //logLoad.error(" [ERROR LECTURA " + NAME_FEED + "] No se han podido realizar las promesas de lectura: " + error); 
	            logger.error( err );

	            //console.log( JSON.stringify(promises) );
	            mysql.end();
	        });
	    })
        .catch(function (err)
        {
            //logLoad.error(" [ERROR LECTURA " + NAME_FEED + "] No se han podido realizar las promesas de lectura: " + error);
            logger.error("Lectura!!!!");
            logger.error( err );

            //console.log( JSON.stringify(promises) );
            mysql.end();
        });
	})
	.fail( function (err)
	{
		logger.error( err );
	});
}
catch(err)
{
    logger.error(" [ERROR LECTURA " + NAME_FEED + "] No se ha podido leer la feed : " + err );
    mysql.end();
};


/* Funcion que lee los deportes */
function readSport( sportData, dictionaryData )
{
	var defer = Q.defer();
	/* Iniciamos la carga del deporte */
	logger.trace( "Iniciamos la carga del feed para: " + sportData.sport );

	/* LLamamos a la feed, para recuperar la información */
	requestFeed( sportData.url, 1 )
	.then(function( result )
	{
		var promisesEvents = [];

		/* Iniciamos la carga del diccionario */
		logger.debug("Terminado el parseo de los datos del feed para: " + sportData.sport );
		logger.trace("Recorremos los eventos de la feed para: " + sportData.sport );
		_.each(result.ContentAPI.Sport[0].SBClass, function( classData )
        {
        	_.each(classData.SBType, function( leagueData )
            {
            	/* Buscamos la liga */
    			var fLeague = _.find( dictionaryData.leagues[ sportData.id ], function( league )
                {
                    return league.handle == leagueData.$.sb_type_id;
                });

        		/* Si encontramos la liga */
        		if( fLeague )
                {
                	/* Recorremos los eventos de la competición */
                    _.each( leagueData.Ev, function( eventData )
                    {
                        /* Leemos el xml del evento */
                        var urlEvent = "http://genfeeds.marcaapuestas.es/odds_feed?key=get_all_markets_for_event&lang=en&ev_id=" + eventData.$.ev_id;
                        
                        /* Guardamos las promesas de los eventos */
                        promisesEvents.push( readEvent( urlEvent, sportData, fLeague, eventData, dictionaryData ) );
                    });
                }
                /* No hemos encontrado la liga */
                else
                {
                    /* Se guardan las ligas no leidas */
                    //logData.writeLog(logLeagues, ID_FEED + '|' + eventData.$.LeagueID + '|' + eventData.$.League, logLoad);
                    //logger.warn("No se encuentra la liga: %s", eventData.$.League );
                }
            });
        });

		Q.allSettled(promisesEvents)
        .timeout(60000)
        .then( function ( res )
        {
        	return defer.resolve();
            logger.info("Finalizada la carga total del deporte:" + sportData.sport );
        })
        .catch(function (err)
        {
            //logger.error(" [ERROR LECTURA " + NAME_FEED + "] No se han podido realizar las promesas de lectura: " + err ); 
            logger.error( err );
            return defer.reject( err );
        });
	})
	.catch(function(err)
	{
		//console.log( err );
		return defer.reject( err );
	});

	
	// request({
	// 	method: 'GET',
	//     url: sportData.url,
	//     gzip: true
	// }, function ( err, response, body )
	// {
	// 	/* Si se ha producido un error */
	// 	if( err )
	// 	{
	// 		logger.fatal("Se ha producido un error en la carga del feed");
	// 		logger.fatal( err );
	// 		return true;
	// 	}
	// 	logger.debug("Terminada la carga del feed para: " + sportData.sport );
	// 	logger.trace("Iniciamos el parseo de los datos del feed para: " + sportData.sport );
	// 	// console.log( response );
	// 	console.log( body );
	// 	/* Parseamos la información de la feed */
	// 	parseString( body, function ( err, result ) 
	// 	{
			
	// 	});
	// });

	return defer.promise;
};

function readEvent( url, sportData, fLeague, eventData, dictionaryData )
{
	var defer = Q.defer();

	/* Iniciamos la carga del deporte */
	logger.trace( "Iniciamos la carga del evento: " + eventData.$.name );

	/* LLamamos a la feed, para recuperar la información */
	requestFeed( url, 1 )
	.then(function( result )
	{
		/* Control para los eventos vacios */
		if( result.ContentAPI.Sport )
		{
			/* Inicializamos mercados */
            var markets = [];

            try
            {
            	if( !result.ContentAPI.Sport[0].SBClass[0].SBType[0].Ev || typeof result.ContentAPI.Sport[0].SBClass[0].SBType[0].Ev == "undefined" )
            		console.log( result );	
            }
            catch( e )
            {
            	console.log( result );
            }
            

			_.each(result.ContentAPI.Sport[0].SBClass[0].SBType[0].Ev[0].Mkt, function(dataMarket)
            {
            	var fMarket = _.find(dictionaryData.markets[ sportData.id ], function( dictionaryMarket )
                    {
                        return dictionaryMarket.str_in_market == utf8.encode(String(dataMarket.$.name));
                    });

            	if( fMarket != undefined )
            	{
            		/* Incializamos apuestas */
	            	var aResults = [];

            		_.each( dataMarket.Seln, function(resultMarket)
                    {
                        if( typeof resultMarket.Price != "undefined" )
                        {
                            aResults.push({
                                id 			: resultMarket.$.seln_id,
                                name 		: resultMarket.$.name,
                                _			: resultMarket.$.seln_sort,
                                odd 		: resultMarket.Price[0].$.dec_prc,
                                placebetlink: resultMarket.Price[0].$.bet_ref
                            });
                        }
                    });

            		markets.push({
    					id: dataMarket.$.id,
			            name: utf8.encode(String(dataMarket.$.name)),
			            bets: aResults
    				});
            	}                    
            });

			/* Si tenemos mercados que insertar */
			if( markets.length > 0 )
			{
				var aParticipant = [];

				if( typeof result.ContentAPI.Sport[0].SBClass[0].SBType[0].Ev[0].Teams != "undefined" )
				{
					_.each(result.ContentAPI.Sport[0].SBClass[0].SBType[0].Ev[0].Teams[0].Team, function(team)
					{
						aParticipant[ team.$.team_order ] = team.$.name;
					});
				}

				var dataEvent = {
					idFeed 		: ID_FEED,
	                nameFeed 	: NAME_FEED,
	                idSport 	: sportData.id,
	                nameSport 	: sportData.sport,
	                idLeague 	: fLeague.handle,
	                nameLeague 	: utf8.encode(String(fLeague.str_league)),
	                idEvent 	: eventData.$.id,
	                nameEvent 	: aParticipant.length == 2 ? utf8.encode(String(aParticipant.join(" vs "))) : utf8.encode(String(eventData.$.name)),
	                eventDate 	: dateUTC.dateFormat( result.ContentAPI.Sport[0].SBClass[0].SBType[0].Ev[0].$.start_time, ID_FEED),
	                eventLink 	: "",
	                bets 		: markets
				};

				listEvents.push( dataEvent );
			}

			return defer.resolve( listEvents );
		}
		/* Se ha producido algun tipo de error en la carga del evento */
		else
		{
			logger.error( 'Se ha producido un error en la conexión del evento: ' + eventData.$.name );

			if( result.ContentAPI.$.status == 'ERROR' )
			{
				logger.error( result.ContentAPI.$.reason );
				return defer.reject(result.ContentAPI.$.reason);
			}
			else
				return defer.reject('Error desconocido en la conexión');
		}	
		
	})
	.catch(function (err)
    {
        logger.error( err );
        return defer.reject( err );
    });

	return defer.promise;
};



var requestFeed = function( url, round )
{
	var defer = Q.defer(),
		req = http.request( url, function(res) 
		{
			//console.log( res.statusCode );

			if( (res.statusCode < 200 || res.statusCode >= 300) && round < 15 )
	        {
	            /* Error. Se reintenta la conexion */
	            round++;
	            return defer.resolve( requestFeed(url, round) );
	        }
	        else if((res.statusCode < 200 || res.statusCode >= 300) && round >= 100 )
	        {
	        	logger.error("ERROR: Fin de reintentos para: " + url);
	            defer.reject("ERROR: Fin de reintentos para: " + url);
	        }
	        else
	        {
		        var  strData = '';
		        res.on('data', function (chunk) 
		        {
		            strData += chunk;
		        });

		        res.on('end', function () 
		        {
		        	/* Parseamos la información de la feed */
					parseString( strData, function ( err, result ) 
					{
						if( err ) 
							return defer.reject( err );
						else
							return defer.resolve( result );
					});
		        });
	        }

	        res.on('error', function(error)
	        {
	            //console.log( error );
	            return defer.reject( e );
	        });

	    });

    req.on('timeout', function () 
    {    
        req.abort();
        return defer.reject("ERROR: Timeout");
        //console.dir("TIMEOUT");
    });

    req.on('error', function (e) 
    {
       	// General error, i.e.
       	//  - ECONNRESET - server closed the socket unexpectedly
       	//  - ECONNREFUSED - server did not listen
       	//  - HPE_INVALID_VERSION
       	//  - HPE_INVALID_STATUS
       	//  - ... (other HPE_* codes) - server returned garbage

       	//console.log( e );

    	/* Error. Se reintenta la conexion */
	    round++;
	    return defer.resolve( requestFeed(url, round) );

       	// req.abort();
       	// //console.dir(e);
       	// return defer.reject( e );
    });

    req.end();

    return defer.promise;
};
