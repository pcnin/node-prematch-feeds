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
    dateUTC     = require('./dates.js'),
    utf8        = require('utf8');

/**********************************************************/
/*                      Settings                          */
/**********************************************************/
/* Configuracion de la feed */
const ID_FEED = 17,
      NAME_FEED = "LUCKIA";
/* Deportes */
var aSBTechSport = [];
    aSBTechSport[1]  = 1;       // Soccer
    aSBTechSport[2]  = 2;       // BasketBall
    aSBTechSport[6]  = 3;       // Tennis
    aSBTechSport[14] = 5;       // Motor Racing
    aSBTechSport[16] = 6;       // Cycling
    aSBTechSport[20] = 7;       // Boxing

/* URL de conexión */
var URL = "http://luckiaxml.sbtech.com/lines.aspx?BranchID=1&BranchID=2&BranchID=6&BranchID=14&BranchID=16&BranchID=20&EventType=0&EventType=1&EventType=2&EventType=8&EventType=38&EventType=60&EventType=61&EventType=62&EventType=109&EventType=111&EventType=131&EventType=144&EventType=158&EventType=184&EventType=200&EventType=201&EventType=270&IncludeLinesIDs=true&OddsStyle=DECIMAL";

log4js.configure({
  appenders: [
    { type: 'console' },
    //{ type: 'file', filename: 'logs/luckia.log', category: 'luckia' }
  ]
});

var logger 		 = log4js.getLogger('luckia'),		
	mysql 		 = new DB(),
	listEvents 	 = [],
    promises     = [];

/* Messages types */
// logger.setLevel('ERROR');

// logger.trace('Entering cheese testing');
// logger.debug('Got cheese.');
// logger.info('Cheese is Gouda.');
// logger.warn('Cheese is quite smelly.');
// logger.error('Cheese is too ripe!');
// logger.fatal('Cheese was breeding ground for listeria.');

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
	    		_.each(result.Events.Event, function( eventData )
            	{
            		/* Si tenemos el deporte */
                    if( aSBTechSport[eventData.$.BranchID] && dictionaryData.leagues[ aSBTechSport[eventData.$.BranchID] ] )
                    {                    
                        /* Buscamos la liga */
            			var fLeague = _.find( dictionaryData.leagues[ aSBTechSport[eventData.$.BranchID] ], function( league )
                        {
                        	//console.log( league.handle+" == "+eventData.$.LeagueID );
                            return league.handle == eventData.$.LeagueID;
                        });

    	                /* Si encontramos la liga */
                		if( fLeague )
                        {
                        	var dataEvent;

                            /* Creamos dependiendo del mercado */
                            switch( eventData.$.EventType )
                            {
                                /* FT -> Resultado 1 X 2 */
                                case '0':
                                    dataEvent = get1X2Market( eventData, 'FT' );
                                break;
                                /* 1st Half -> Resultado 1 X 2 - 1er Tiempo */
                                case '1':
                                    dataEvent = get1X2Market( eventData, '1st Half' );
                                break;
                                /* 2st Half -> Resultado 1 X 2 - 2º Tiempo */
                                case '2':
                                    dataEvent = get1X2Market( eventData, '2st Half' );
                                break;
                                /* OutRight -> Ganador de la competición */
                                case '8':
                                    dataEvent = getOthersMarket( eventData, 'Winner' );
                                break;
                                /* Odd/Even */
                                // case '38':
                                //     dataEvent = getOthersMarket( eventData, 'Odd/Even' );
                                // break;
                                /* Exact Score -> Marcador Exacto */
                                case '60':
                                    dataEvent = getOthersMarket( eventData, 'Exact Score' );
                                break;
                                /* Double Chance -> Doble Oportunidad */
                                case '61':
                                    dataEvent = getOthersMarket( eventData, 'Double Chance' );
                                break;
                                /* Halftime/Fulltime -> Descanso / Final */
                                case '62':
                                    dataEvent = getOthersMarket( eventData, 'Halftime/Fulltime' );
                                break;
                                /* Winner */
                                case '109':
                                    dataEvent = getOthersMarket( eventData, 'LineType1' );
                                break;
                                /* Group Winner -> Ganador del grupo */
                                case '111':
                                    dataEvent = getOthersMarket( eventData, 'LineType1' );
                                break;
                                /* Constructors */
                                case '131':
                                    dataEvent = getOthersMarket( eventData, 'LineType1' );
                                break;
                                /* Exact Score 1st Half -> Marcador Exacto 1ª Mitad */
                                case '144':
                                    dataEvent = getOthersMarket( eventData, 'Exact Score 1st Half' );
                                break;
                                /* Double Chance 1st Half */
                                // case '145':
                                //     dataEvent = getOthersMarket( eventData, 'Double Chance 1st Half' );
                                // break;
                                /* Both to Score -> ¿Marcarán los dos equipos en el partido? */
                                case '158':
                                    dataEvent = getOthersMarket( eventData, 'Both to Score' );
                                break;
                                /* Top Goalscorer -> Máximo goleador */
                                case '184':
                                    dataEvent = getOthersMarket( eventData, 'LineType1' );
                                break;
                                /* getTotalGoalsMarket Goals -> Goles +/- de */
                                case '200':
                                    dataEvent = getTotalGoalsMarket( eventData, 'Total Goals' );
                                break;
                                /* Total Goals 1st Half -> Goles +/- de 1ª Mitad */
                                case '201':
                                    dataEvent = getTotalGoalsMarket( eventData, 'Total Goals 1st Half' );
                                break;
                                /* 3W Handicap -> Handicap */
                                case '270':
                                    dataEvent = getHandicapMarket( eventData, '3W Handicap' );
                                break;
                            }

                            /* Si tenemos datos los añadimos */
                            if( dataEvent )
                            {
                            	listEvents.push( dataEvent );
                            	//logger.info( dataEvent );
                            }
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
                /* Terminando de recorrer los eventos */
                logger.trace("Terminamos de recorrer los eventos de la feed" );

                /* Cuando todas las promesas acaban, se cierra el pool y se liberan recursos */
                Q.allSettled(promises)
                .timeout(60000)
                .then( function ( res )
                {
                    console.log( res.length + " - "+  listEvents.length );
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
                })

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
}

/* Funciones de Mercados */
/* 0 -> FT               */
/* 1 -> 1st Half         */
/* 2 -> 2st Half         */
function get1X2Market( eventData, marketType )
{   
    /* Si no tiene MoneyLine no es una apuesta válida */
    /* Tiene que tener tambien apuestas para el Local y Visitante */
    if( eventData.MoneyLine && eventData.MoneyLine[0].$.Home && eventData.MoneyLine[0].$.Away)
    {
        var findData = -1;
        for( var nCont = 0, len = listEvents.length; nCont < len && findData == -1; nCont++ )
            if( listEvents[nCont].idEvent == eventData.$.MEID ) findData = nCont;

        /* Creamos el nombre del evento */
        var aParticipants = new Array();
        /* Home */
        aParticipants[0] = eventData.Participants[0].Participant1[0].$.Home_Visiting == 'Home' ?
                               eventData.Participants[0].Participant1[0].$.Name :
                               eventData.Participants[0].Participant2[0].$.Name;
        /* Visitante */
        aParticipants[1] = eventData.Participants[0].Participant2[0].$.Home_Visiting == 'Visiting' ?
                               eventData.Participants[0].Participant2[0].$.Name :
                               eventData.Participants[0].Participant1[0].$.Name;
        /* Recogemos sus cuotas */
        var aResults = [];

        /* Apuesta del local */
        aResults[0] = {
            id           : eventData.MoneyLine[0].$.Home_LineID,
            name         : utf8.encode(String(aParticipants[0])),
            odd          : eventData.MoneyLine[0].$.Home,
            placebetlink : eventData.MoneyLine[0].$.Home_LineID
        };
        /* Si hay empate */
        if( eventData.MoneyLine[0].$.Draw )
        {
            aResults.push({
                id           : eventData.MoneyLine[0].$.Draw_LineID,
                name         : 'Empate',
                odd          : eventData.MoneyLine[0].$.Draw,
                placebetlink : eventData.MoneyLine[0].$.Draw_LineID
            });
        }
        /* Apuesta del visitante */
        aResults.push({
            id           : eventData.MoneyLine[0].$.Away_LineID,
            name         : utf8.encode(String(aParticipants[1])),
            odd          : eventData.MoneyLine[0].$.Away,
            placebetlink : eventData.MoneyLine[0].$.Away_LineID
        });
        /* Mercados */
        var aBets = {
            id: eventData.$.ID,
            name: utf8.encode(String(marketType)),
            bets: aResults
        };

        /* Añadimos un nuevo Mercado */
        if( findData >= 0 )
        {
            listEvents[findData].bets.push( aBets );
        }
        /* Creamos el evento con su mercado */
        else
        {
            return {
                idFeed : ID_FEED,
                nameFeed : NAME_FEED,
                idSport : aSBTechSport[eventData.$.BranchID],
                nameSport : eventData.$.Sport,
                idLeague : eventData.$.LeagueID,
                nameLeague : utf8.encode(String(eventData.$.League)),
                idEvent : eventData.$.MEID,
                nameEvent : utf8.encode(String(aParticipants[0] + ' vs ' + aParticipants[1])),
                eventDate : dateUTC.dateFormat( eventData.$.DateTimeGMT, ID_FEED),
                eventLink : "",
                bets : [aBets]
            }
            
        }
    }
};

function getTotalGoalsMarket( eventData, marketType )
{   
    /* Si no tiene Total no es una apuesta válida */
    if( eventData.Total )
    {
        var findData = -1;
        for( var nCont = 0, len = listEvents.length; nCont < len && findData == -1; nCont++ )
            if( listEvents[nCont].idEvent == eventData.$.MEID ) findData = nCont;

        /* Creamos el nombre del evento */
        var aParticipants = new Array();
        /* Home */
        aParticipants[0] = eventData.Participants[0].Participant1[0].$.Home_Visiting == 'Home' ?
                               eventData.Participants[0].Participant1[0].$.Name :
                               eventData.Participants[0].Participant2[0].$.Name;
        /* Visitante */
        aParticipants[1] = eventData.Participants[0].Participant2[0].$.Home_Visiting == 'Visiting' ?
                               eventData.Participants[0].Participant2[0].$.Name :
                               eventData.Participants[0].Participant1[0].$.Name;
        /* Recogemos sus cuotas */
        // <Total Points="2.5" Over="2.55" Under="1.54" Over_LineID="R228518053" Under_LineID="R228518057"/>
        var aResults = [{
                id          : 0,
                name        : utf8.encode(String('P ' + eventData.Total[0].$.Points)),
                odd         : eventData.Total[0].$.Over,
                placebetlink: eventData.Total[0].$.Over_LineID
            },{
                id          : 0,
                name        : utf8.encode(String('M ' + eventData.Total[0].$.Points)),
                odd         : eventData.Total[0].$.Under,
                placebetlink: eventData.Total[0].$.Under_LineID
            }];

        /* Mercados */
        var aBets = {
            id: eventData.$.ID,
            name: utf8.encode(String(marketType)),
            bets: aResults
        };

        /* Añadimos un nuevo Mercado */
        if( findData >= 0 )
        {
            listEvents[findData].bets.push( aBets );
        }
        /* Creamos el evento con su mercado */
        else
        {
            return {
                idFeed : ID_FEED,
                nameFeed : NAME_FEED,
                idSport : aSBTechSport[eventData.$.BranchID],
                nameSport : eventData.$.Sport,
                idLeague : eventData.$.LeagueID,
                nameLeague : utf8.encode(String(eventData.$.League)),
                idEvent : eventData.$.MEID,
                nameEvent : utf8.encode(String(aParticipants[0] + ' vs ' + aParticipants[1])),
                eventDate : dateUTC.dateFormat( eventData.$.DateTimeGMT, ID_FEED),
                eventLink : "",
                bets : [aBets]
            }
            
        }
    }
};

function getHandicapMarket( eventData, marketType )
{
    var findData = -1;
        for( var nCont = 0, len = listEvents.length; nCont < len && findData == -1; nCont++ )
            if( listEvents[nCont].nameEvent == utf8.encode(String(eventData.$.EventName)) ) findData = nCont;

    /* Recogemos sus cuotas */
    var aResults = [];

    /* Recorremos sus eventos */
    _.each(eventData.Participants[0].Participant, function( participant )
    {
        _.each( participant.Odds, function( odd )
        {
            var aux = participant.$.Name.split('(')[1],
                number = aux.substr( 0, aux.length-1 ),
                result = number.substr(0,1) == '-' ? number : '+' + number;

            switch( odd.$.TypeName )
            {
                case 'LineType1':
                    result += '/1';
                break;
                case 'LineType2':
                    result += '/x';
                break;
                case 'LineType3':
                    result += '/2';
                break;
            }

            aResults.push({
                id           : 0,
                name         : utf8.encode(String(result)),
                odd          : odd.$.OddsValue,
                placebetlink : odd.$.LineID
            });

        });
    });

    /* Mercados */
    var aBets = {
        id: eventData.$.EventType,
        name: utf8.encode(String(marketType)),
        bets: aResults
    };

    /* Añadimos un nuevo Mercado */
    if( findData >= 0 )
    {
        listEvents[findData].bets.push( aBets );
    }
    /* Creamos el evento con su mercado */
    else
    {
        return {
            idFeed : ID_FEED,
            nameFeed : NAME_FEED,
            idSport : aSBTechSport[eventData.$.BranchID],
            nameSport : eventData.$.Branch,
            idLeague : eventData.$.LeagueID,
            nameLeague : utf8.encode(String(eventData.$.League)),
            idEvent : eventData.$.QAID,
            nameEvent : utf8.encode(String(eventData.$.EventName)),
            eventDate : dateUTC.dateFormat( eventData.$.DateTimeGMT, ID_FEED),
            eventLink : "",
            bets : [aBets]
        }
    }

};

/* 8  -> OutRight  */
/* 38 -> Odd/Event */
function getOthersMarket( eventData, marketType )
{
    if( eventData.Participants[0].Participant && eventData.Participants[0].Participant.length )
    {
        var findData = -1;
        for( var nCont = 0, len = listEvents.length; nCont < len && findData == -1; nCont++ )
            if( listEvents[nCont].nameEvent == utf8.encode(String(eventData.$.EventName)) ) findData = nCont;

        /* Recogemos sus cuotas */
        var aResults = [];

        /* Recorremos sus eventos */
        _.each(eventData.Participants[0].Participant, function( participant )
        {
            _.each( participant.Odds, function( odd )
            {
                if( odd.$.TypeName == marketType )
                {
                    aResults.push({
                        id           : 0,
                        name         : utf8.encode(String(participant.$.Name)),
                        odd          : odd.$.OddsValue,
                        placebetlink : odd.$.LineID
                    });
                }
            });
        });

        /* Cambiamos el nombre del mercado para los LineType1 */
        if( marketType == 'LineType1' )
        {
            switch( eventData.$.EventType )
            {
                case '109':
                    marketType = 'Winner';
                break;
                case '111':
                    marketType = 'Group Winner';
                break;
                case '131':
                    marketType = 'Constructors';
                break;
                case '184':
                    marketType = 'Top Goalscorer';
                break;
            }
        }

        /* Mercados */
        var aBets = {
            id: eventData.$.EventType,
            name: utf8.encode(String(marketType)),
            bets: aResults
        };

        /* Añadimos un nuevo Mercado */
        if( findData >= 0 )
        {
            listEvents[findData].bets.push( aBets );
        }
        /* Creamos el evento con su mercado */
        else
        {
            return {
                idFeed : ID_FEED,
                nameFeed : NAME_FEED,
                idSport : aSBTechSport[eventData.$.BranchID],
                nameSport : eventData.$.Branch,
                idLeague : eventData.$.LeagueID,
                nameLeague : utf8.encode(String(eventData.$.League)),
                idEvent : eventData.$.QAID,
                nameEvent : utf8.encode(String(eventData.$.EventName)),
                eventDate : dateUTC.dateFormat( eventData.$.DateTimeGMT, ID_FEED),
                eventLink : "",
                bets : [aBets]
            }
        }
    }
};