/**********************************************************/
/*                      Includes                          */
/**********************************************************/
var log4js  	= require('log4js'),
	parseString = require('xml2js').parseString,
	Q  			= require('q'),
	request 	= require('request'),
	_			= require('underscore'),
	DB 			= require('./bbdd/connection.js'),
    record      = require("./record.js"),
	dictionary  = require('./dictionary.js'),
    dateUTC     = require('./dates.js');

/**********************************************************/
/*                      Settings                          */
/**********************************************************/
const URL = "https://www.paf.es/sportsbettingfeed/prematch.xml";	// URL de conexión

/* Deportes */
var aSBTechSport = [];
    // aSBTechSport[1]  = 'FOOTBALL';       // Soccer
    // aSBTechSport[2]  = 'BASKETBALL';     // BasketBall
    aSBTechSport[3]  = 'TENNIS';       	 // Tennis
	// aSBTechSport[4]  = 'MOTORCYCLING';   // Motor Racing
 //    aSBTechSport[5]  = 'FORMULA_1';      // Motor Racing
 //    aSBTechSport[6]  = 'CYCLING';        // Cycling
 //    aSBTechSport[7]  = 'BOXING';         // Boxing
 //   	aSBTechSport[8]  = 'HANDBALL';       // HANDBALL

log4js.configure({
  appenders: [
    { type: 'console' },
    //{ type: 'file', filename: 'logs/paf.log', category: 'paf' }
  ]
});

var logger 		 = log4js.getLogger('paf'),		
	mysql 		 = new DB(),
	listEvents 	 = [],
    promises     = [];

/* Iniciamos la carga del diccionario */
logger.trace("Iniciamos la carga del feed" );
try
{
	/* LLamamos a la feed, para recuperar la información */
	request({
		method: 'GET',
	    uri: URL,
	    gzip: true
	}, function ( err, response, body )
	{
		/* Si se ha producido un error */
		if( err )
		{
			logger.fatal("Se ha producido un error en la carga del feed");
			logger.fatal( err );
			return true;
		}
		logger.debug("Terminada la carga del feed" );
		logger.trace("Iniciamos el parseo de los datos del feed" );
		/* Parseamos la información de la feed */
		parseString( body, function ( err, result ) 
		{
			/* Iniciamos la carga del diccionario */
			logger.debug("Terminado el parseo de los datos del feed" );

			makeFeedData( result )
			.then( function( res )
			{
				console.log( JSON.stringify(res) );

				_.each( res, function( item )
				{
					/* Recorremos todos los eventos y vamos tramitandolos */
		            _.map( item.events, function( event )
		            {
		                /* Creamos el diccionario segun el evento.deporte */
		                var dictionarySport = {
		                    leagues: item.dictionaryData.leagues[ event.idSport ],
		                    events: item.dictionaryData.events[ event.idSport ],
		                    participants: item.dictionaryData.participants[ event.idSport ],
		                    markets: item.dictionaryData.markets[ event.idSport ]
		                }
		                promises.push( record.event( event, dictionarySport, mysql, logger ) );
		            });
				});
				
	            /* Cuando todas las promesas acaban, se cierra el pool y se liberan recursos */
	            Q.allSettled(promises)
	            .timeout(15000)
	            .then( function ( res )
	            {
	                // console.log( JSON.stringify(res) );
	                // console.log( JSON.stringify(listEvents) );
	                // console.log("terminado>>>>>");

	                mysql.end();
	                logger.info("Finalizada la carga total de la feed" );
	            })
	            .catch(function (err)
	            {
	                //logLoad.error(" [ERROR LECTURA " + NAME_FEED + "] No se han podido realizar las promesas de lectura: " + error); 
	                logger.error( err );

	                //console.log( JSON.stringify(promises) );
	                mysql.end();
	            });
			})
			.catch( function( err )
			{
				logger.error( err );
				mysql.end();
			});		
		});
	});
}
catch(err)
{
    logger.error(" [ERROR LECTURA " + NAME_FEED + "] No se ha podido leer la feed : " + err );  
}


function makeFeedData( result )
{
	var deferer = Q.defer(),
		exit = [];

	/* Configuracion de la feed */
	var listFeeds = [{
			ID_FEED: 2,
			NAME_FEED: "PAF"
		},{
			ID_FEED: 9,
			NAME_FEED: "SUERTIA"
		},{
			ID_FEED: 12,
			NAME_FEED: "888"
		}];

	/* Recorremos cada una de las kambis */
	_.each( listFeeds, function(feed)
	{
		var listEvents = [];

		logger.trace("Iniciamos la carga del diccionario: " + feed.NAME_FEED );
		dictionary.getDictionary( feed.ID_FEED, mysql )
		.then(function( dictionaryData )
		{
			/* Recorremos sus eventos */
			logger.debug("Terminado la carga del diccionario: " + feed.NAME_FEED );
			logger.trace("Recorremos los eventos de la feed: " + feed.NAME_FEED );
			// { paf: { message: [ [Object] ] } }
    		_.each(result.paf.message[0].event, function( eventData )
        	{
        		var sportID = _.indexOf( aSBTechSport, eventData.sport[0] );

        		/* Si encontramos el deporte */
        		if( sportID != -1 )
        		{
        			/* Buscamos la liga */
        			var fLeague = _.find( dictionaryData.leagues[ sportID ], function( league )
                    {
                    	if( eventData.group && eventData.group[0].group && eventData.group[0].group[0].group )
                        	return league.handle == eventData.group[0].group[0].group[0].$.id;
                    });

            		if( fLeague != undefined )
            		{
            			/* Inicializamos datos */
            			var markets = [];

            			_.each( eventData.product, function( market )
            			{
            				/* Incializamos apuestas */
            				var aResults = [];

            				_.each( market.choice, function( bet )
            				{
            					aResults.push({
					                id           : bet.$.id,
					                name         : bet.$.name,
					                _			 : typeof bet.odds[0] == "object" ? bet.odds[0].$.line/1000 : "",
					                odd          : typeof bet.odds[0] == "string" ? bet.odds[0]/1000 : bet.odds[0]._/1000,
					                placebetlink : ""
					            });
            				});

            				markets.push({
            					id: market.$.id,
					            name: market.$.name,
					            bets: aResults
            				});
            			});

    					dataEvent = {
    						idFeed : feed.ID_FEED,
			                nameFeed : feed.NAME_FEED,
			                idSport : sportID,
			                nameSport : aSBTechSport[sportID],
			                idLeague : fLeague.handle,
			                nameLeague : fLeague.str_league,
			                idEvent : eventData.$.id,
			                nameEvent : eventData.$.name,
			                eventDate : dateUTC.dateFormat( eventData.start_time[0], feed.ID_FEED),
			                eventLink : "",
			                bets : [markets]
    					};

    					listEvents.push( dataEvent );

    					

            		}
            		/* No hemos encontrado la liga */
                    else
                    {
                        /* Se guardan las ligas no leidas */
                        //logData.writeLog(logLeagues, ID_FEED + '|' + eventData.$.LeagueID + '|' + eventData.$.League, logLoad);
                        //logger.warn("No se encuentra la liga: %s", eventData.$.League );
                    }
        		} 
        	});
			/* Terminando de recorrer los eventos */
            logger.trace("Terminamos de recorrer los eventos de la feed: " + feed.NAME_FEED );

            exit.push({
            	events: listEvents,
            	dictionaryData: dictionaryData
            });

            /* Si es el ultimo elemento de los recorridos */
			if( listFeeds.length == exit.length )
				return deferer.resolve( exit );
		})
		.fail( function (err)
		{
			logger.error( err );
		});
	});

	return deferer.promise;	
};