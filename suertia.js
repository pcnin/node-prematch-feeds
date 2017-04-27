/**********************************************************/
/*                      Includes                          */
/**********************************************************/
var log4js  	= require('log4js'),
	parseString = require('xml2js').parseString,
	Q  			= require('q'),
	request 	= require('request'),
	_			= require('underscore'),
	DB 			= require('./bbdd/connection.js'),
    record      = require('./record.js'),
	dictionary  = require('./dictionary.js'),
    dateUTC     = require('./dates.js'),
    files		= require('./dataFile.js'),
    utf8    	= require('utf8');

/**********************************************************/
/*                      Settings                          */
/**********************************************************/
const NAME_FEED = "SUERTIA",
	  ID_FEED = 9,
	  URL = "https://www.paf.es/sportsbettingfeed/prematch.xml";	// URL de conexión

/* Deportes */
var aSBTechSport = [];
    aSBTechSport[1]  = 'FOOTBALL';       // Soccer
    aSBTechSport[2]  = 'BASKETBALL';     // BasketBall
    aSBTechSport[3]  = 'TENNIS';       	 // Tennis
	aSBTechSport[4]  = 'MOTORCYCLING';   // Motor Racing
    aSBTechSport[5]  = 'FORMULA_1';      // Motor Racing
    aSBTechSport[6]  = 'CYCLING';        // Cycling
    aSBTechSport[7]  = 'BOXING';         // Boxing
   	// aSBTechSport[8]  = 'HANDBALL';    // HANDBALL

log4js.configure({
  appenders: [
    { type: 'console' },
    { type: 'file', 
      filename: files.path.logs + NAME_FEED + ".log", 
      category: 'suertia',
      maxLogSize: 10485760 }
  ]
});

var logger 		 = log4js.getLogger('suertia'),		
	mysql 		 = new DB(),
	listEvents 	 = [],
    promises     = [],
    logLeagues 	 = files.path.conf + NAME_FEED.replace(" ","").toLowerCase() + "_leagues.out.log",
    logDataFeed  = files.path.logs + NAME_FEED + ".data.log";

/* Borrar el log de las ligas no leidas, el blog de la feed y el log de los datos leidos */
files.open( logLeagues, 'w' );
files.open( logDataFeed, 'w' );

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
			logger.trace("Iniciamos la carga del diccionario" );
			dictionary.getDictionary( ID_FEED, mysql )
			.then(function( dictionaryData )
			{
				/* Recorremos sus eventos */
				logger.debug("Terminado la carga del diccionario");
				logger.trace("Recorremos los eventos de la feed" );
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
	            				var fMarket = _.find(dictionaryData.markets[ sportID ], function( dictionaryMarket )
			                    {
			                        return dictionaryMarket.str_in_market == utf8.encode(String(market.$.name + " - " + market.$.type));
			                    });

	            				if( fMarket != undefined )
	            				{
		            				/* Incializamos apuestas */
		            				var aResults = [];

		            				_.each( market.choice, function( bet )
		            				{
		            					aResults.push({
							                id           : bet.$.id,
							                name         : utf8.encode(String(bet.$.name)),
							                _			 : typeof bet.odds[0] == "object" ? bet.odds[0].$.line/1000 : "",
							                odd          : typeof bet.odds[0] == "string" ? bet.odds[0]/1000 : bet.odds[0]._/1000,
							                placebetlink : ""
							            });
		            				});

		            				markets.push({
		            					id: market.$.id,
							            name: utf8.encode(String(market.$.name + " - " + market.$.type)),
							            bets: aResults
		            				});
		            			}
	            			});

							/* Si tenemos mercados que insertar */
							if( markets.length > 0 )
							{
								/* Buscamos el evento, por si ya lo tenemos */
								var findData = -1;
						        for( var nCont = 0, len = listEvents.length; nCont < len && findData == -1; nCont++ )
						            if( listEvents[nCont].idEvent == eventData.$.id ) findData = nCont;

						        /* Añadimos un nuevo Mercado */
						        if( findData >= 0 )
						        {
						            listEvents[findData].bets.concat( markets );
						        }
						        /* Creamos el evento con su mercado */
						        else
						        {
		        					var dataEvent = {
		        						idFeed : ID_FEED,
						                nameFeed : NAME_FEED,
						                idSport : sportID,
						                nameSport : aSBTechSport[sportID],
						                idLeague : fLeague.handle,
						                nameLeague : utf8.encode(String(fLeague.str_league)),
						                idEvent : eventData.$.id,
						                nameEvent : utf8.encode(String(eventData.$.name)),
						                eventDate : dateUTC.dateFormat( eventData.start_time[0], ID_FEED),
						                eventLink : "",
						                bets : markets
		        					};

		        					listEvents.push( dataEvent );
		        				}
	        				}
	            		}
	            		/* No hemos encontrado la liga */
                        else
                        {
                            /* Se guardan las ligas no leidas */
                            if( eventData.group && eventData.group[0].group && eventData.group[0].group[0].group )
                            {
                            	files.write( logLeagues, ID_FEED + '|' + eventData.group[0].group[0].group[0].$.id + '|' + eventData.group[0].group[0].group[0].$.name );
                            	logger.warn( "No se encuentra la liga: %s", eventData.group[0].group[0].group[0].$.name );
                            }
                        }
            		} 
            	});
				/* Terminando de recorrer los eventos */
                logger.trace("Terminamos de recorrer los eventos de la feed" );

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
                    promises.push( record.event( event, dictionarySport, mysql, logger ) );
                });

                /* Fichero de datos */
                files.write( logDataFeed, JSON.stringify(listEvents) );

                /* Cuando todas las promesas acaban, se cierra el pool y se liberan recursos */
	            Q.allSettled(promises)
	            .timeout(60000)
	            .then( function ( res )
	            {
	            	console.log( res.length + " - " + listEvents.length );
	                //console.log( JSON.stringify(res) );
	                //console.log( JSON.stringify(listEvents) );
	                //console.log("terminado>>>>>");

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
			.fail( function (err)
			{
				logger.error( err );
			});
		});
	});
}
catch(err)
{
    logger.error(" [ERROR LECTURA " + NAME_FEED + "] No se ha podido leer la feed : " + err );
    mysql.end();
};