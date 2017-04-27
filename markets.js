'use strict';
/* Librerías */
var _ 		= require('underscore'),
	utf8    = require('utf8');

/* MERCADO 1 X 2 */
var m1X2 = module.exports.m1X2 = function( strEvent, bets, participantData, participants, fMarket, reverse, logger )
{
	try
	{
		/* Array de resultados */
		var aResults = [];
		/* Recorremos las apuestas */
		_.each( bets, function( oddData )
		{
			var translate = "";

			switch( oddData.name.trim().toLowerCase() )
            {
                /* Traducimos los patrones de las casas */
                case "1":
                    translate = "1";
                break;
                case "2":
                    translate = "2";
                break;
                case "x":
                    translate = "X";
                break;
                case "empate":
                    translate = "X";
                break;
                case "draw":
                    translate = "X";
                break;
                default:
                	/* Buscar el participante */
                	var regExp 	 = /\s\(.+\)*|\sGanador/,
                    	strName  = oddData.name.replace(regExp, "").trim().toLowerCase(),
                        aParticipant = strEvent.replace(regExp, "").split(" vs ");
                    	// strLocal_feed = typeof participantData.home == "array" ? participantData.home[0].participant_feed+"/"+participantData.home[1].participant_feed : participantData.home.participant_feed,
                    	// strAway_feed  = typeof participantData.away == "array" ? participantData.away[0].participant_feed+"/"+participantData.away[1].participant_feed : participantData.away.participant_feed;

                    /* Buscamos el local */
                    if( strName == aParticipant[0].trim().toLowerCase() )
                    {
                        translate = "1";
                        //logger.warn( strName + " == " + strLocal_feed.trim().toLowerCase() + " --- Ponemos el participante a 1 (dateEvent[0]) en: " + strEvent );
                    }
                    /* Buscamos el visitante */
                    else if( strName == aParticipant[1].trim().toLowerCase() ) 
                    {
                        translate = "2";
                        //logger.warn( strName + " == " + strAway_feed.trim().toLowerCase() + " --- Ponemos el participante a 2 (dateEvent[1]) en: " + strEvent );
                    }
                    /* Como no hemos encontrado el nombre del participante en el que viene en el evento vamos a buscarlo en el diccionario */
                    else
                    {
                    	var participantFind = _.find( participants, function (participant)
                        {
                            return participant.participant_feed.trim().toLowerCase() == strName;
                        });

                        if( participantFind != undefined )
                        {
                            if( participantData.home.participant.toLowerCase() == participantFind.participant.toLowerCase() )
                            {
                                translate = "1";
                                logger.warn( strLocal + "==" + participantFind.participant + " --- Ponemos el participante a 1 en: " + strEvent );
                            }
                            else if( participantData.away.participant.toLowerCase() == participantFind.participant.toLowerCase() )
                            {
                                translate = "2";
                                logger.warn( strAway + "==" + participantFind.participant + " --- Ponemos el participante a 2 en: " + strEvent );
                            }
                            else
                            {
                                logger.warn("[WARNING] No se encontro el participante: " + strName + " al comparararlos en: " + strEvent + " con: " + participantFind );
                                //logData.writeLog(logParticipants, idSport + "|" + idFeed + "|" + strName , logFeed);
                            }
                        }
                        else
                        {
                           logger.warn("[WARNING] No se encontro el participante: " + strName + " al comparararlos en: " + strEvent );
                           //logData.writeLog(logParticipants, idSport + "|" + idFeed + "|" + strName , logFeed);
                        }

                    }
                break;
            }

            /* Modificamos sus cuotas, para los eventos que vengan cambiados */
            if(reverse)
            {
                if(translate == "1") translate = "2";
                else if(translate == "2") translate = "1";
            }
            
            /* Solo si hemos podido traducir las cuotas se guardan */
            if( translate != "" )
            {
            	aResults.push({
	           		id: oddData.id,
		        	name: translate,
		        	name_feed: oddData.name,
		        	odd: oddData.odd,
		        	placebetlink: oddData.placebetlink
               });
            }
		});

		/* Para futbol se necesitan 3 cuotas. Para baloncesto, tenis o boxeo 2 */
        if( ( fMarket.id_market == 7 || fMarket.id_market == 23) && aResults.length != 2 ||
             (fMarket.id_market != 7 && fMarket.id_market != 23) && aResults.length != 3 ||
             aResults.length == 0 )
        {
        	/* Reiniciamos el resultado para no devolver ninguno */
            aResults = [];
            logger.warn("[ERROR] No tenemos cantidad de apuestas suficientes en el mercado 1X2 para el evento " + strEvent );
            return aResults;
        }
        else
            return aResults;
	}
	catch( e )
	{
		logger.error("[ERROR] al realizar el mercado 1X2 para el evento: " + strEvent );
		logger.error( e );
		return aResults;
	}
};
/* MERCADO DOBLE OPORTUNIDAD */
var mDoubleChance = module.exports.mDoubleChance = function( data, bets, participantData, participants, fMarket, reverse, logger )
{
    /*   PATRON DE BESGAM   */
    /*************************
    1 -> Local o empate (1X - X1)
    X -> visitante o empate (X2 - 2X)
    2 -> local o visitante (12 - 21)
    *************************/
    try
    {
        /* Array de resultados */
        var aResults = [];
        /* Recorremos las apuestas */
        _.each( bets, function( oddData )
        {
            var translate = "";

            /* Se busca el patron de cada feed para ajustarlo a nuestro resultado */
            switch( data.idFeed )
            {
                /* PAF */
                case 2:
                /* Suertia */
                case 9:
                /* 888 */
                case 12:
                    /* Nos envian el formato correcto */
                break;
                case 17:
                    oddData.name = oddData.name.split(' or ').join('');
                break;
            };

            switch( oddData.name )
            {
                case "1X":
                    translate = "1";
                break;
                case "X1":
                    translate = "1";
                break;
                case "X2":
                    translate = "X";
                break;
                case "2X":
                    translate = "X";
                break;
                case "12":
                    translate = "2";
                break;
                case "21":
                    translate = "2";
                break; 
                default:
                    translate = "";
                break;
            }

            /* Giramos las cuotas */
            if(reverse) translate = (translate == "1") ? "X" : (translate) == "X" ? "1" : translate;

            /* Solo si hemos podido traducir las cuotas se guardan */
            if( translate != "" )
            {
                aResults.push({
                    id: oddData.id,
                    name: translate,
                    name_feed: oddData.name,
                    odd: oddData.odd,
                    placebetlink: oddData.placebetlink
               });
            }

        });

        /* Debe haber 3 cuotas en este mercado */
        if( aResults.length != 3 )
        {
            aResults = [];
            logger.warn("[ERROR] No tenemos cantidad de apuestas suficientes en el mercado DOBLE OPORTUNIDAD para el evento " + data.nameEvent );
            return aResults;
        }
        else
            return aResults;
    }
    catch( e )
    {
        logger.error("[ERROR] al realizar el mercado DOBLE OPORTUNIDAD para el evento: " + data.nameEvent );
        logger.error( e );
        return aResults;
    }
};


/* MERCADO DESCANSO / FINAL */
var mHalfFull = module.exports.mHalfFull = function( data, bets, participantData, participants, fMarket, reverse, logger )
{
    /*   PATRON DE BESGAM   */
    /*************************
    1/X -> Local o empate 
    2/X -> visitante o empate
    X/X -> empate o empate
    1/2 -> local o visitante 
    1/1 -> localo o local 
    2/1 -> visitante o local
    2/2 -> visitante o visitante
    X/1 -> empate o local
    X/2 -> empate o visitante
    *************************/
    try
    {
        /* Array de resultados */
        var aResults = [];
        /* Recorremos las apuestas */
        _.each( bets, function( oddData )
        {
            var translate = "";

            /* Se busca el patron de cada feed para ajustarlo a nuestro resultado */
            switch( data.idFeed )
            {
                case  2:    // PAF
                case 17:    // LUCKIA
                    translate = oddData.name.trim();
                break;
            }

            /* Girar las cuotas */
            if( reverse ) translate = reverseString(translate);

            /* Solo si hemos podido traducir las cuotas se guardan */
            if( oddData.name != "" && translate != "" && translate.length == 3 )
            {
                aResults.push({
                    id: oddData.id,
                    name: translate,
                    name_feed: oddData.name,
                    odd: oddData.odd,
                    placebetlink: oddData.placebetlink
               });
            }
        });

        /* En este mercado debe haber 9 cuotas */
        if( aResults.length != 9 )
        {
            aResults = [];
            logger.warn("[ERROR] No tenemos cantidad de apuestas suficientes en el mercado DESCANSO / FINAL para el evento " + data.nameEvent );
            return aResults;
        }
        else
            return aResults;
    }
    catch( e )
    {
        logger.error("[ERROR] al realizar el mercado DESCANSO / FINAL para el evento: " + data.nameEvent );
        logger.error( e );
        return aResults;
    }
};

/* MERCADO GANADOR/ MAXIMO GOLEADOR */
var mLoadWinner = module.exports.mLoadWinner = function( data, bets, participants, logger )
{
    try
    {
        /* Array de resultados */
        var aResults = [];

        /* Recorremos las apuestas */
        _.each( bets, function( oddData )
        {
            var regExp      = /\s\(.+\)*|\s- Yes/,
                nameSearch  = oddData.name.replace( regExp, "" ).trim();
            
            /* Hay que buscar el participante */
            var participantFind = _.find( participants, function(participant)
            {
                return participant.participant_feed == nameSearch;
            });

            if( participantFind != undefined )
            {
                aResults.push({
                    id: oddData.id,
                    name: participantFind.participant,
                    name_feed: oddData.name,
                    odd: oddData.odd,
                    placebetlink: oddData.placebetlink
               });
            }
            else
            {
                logger.warn("[WARNING] No se encontro el participante: " + nameSearch + " al comparararlos en: " + data.nameEvent);
                //logData.writeLog(logParticipants, idSport + "|" + idFeed + "|" + nameSearch, logFeed);
            }
        });

        return aResults;
    }
    catch( e )
    {
        logger.error("[ERROR] al realizar el mercado GANADOR / MAXIMO GOLEADOR para el evento: " + data.nameEvent );
        logger.error( e );
        return aResults;
    }
};

/* MERCADO MARCADOR CORRECTO */
var mCorrectScore = module.exports.mCorrectScore =  function( data, bets, participantData, participants, fMarket, reverse, logger )
{
    /*      PATRON DE BESGAM      */
    /*******************************
        0-0                        
        1-0 (1 local, 0 visitante) 
    /******************************/

    /* PAF      0-0               */
    /* LUCKIA   0:0               */

    /* Se recorren los resultados */
    try
    {
        /* Array de resultados */
        var aResults = [];
            
        /* Recorremos las apuestas */
        _.each( bets, function( oddData )
        {
            var score = [],
                regExp = null,
                translate = "";

            /* Se busca el patron de cada feed para ajustarlo a nuestro resultado */
            switch( data.idFeed )
            {
                /* PAF */
                case 2: // 0-0
                /* Suertia */
                case 9:
                /* 888 */
                case 12:
                    /*Nos envian el formato correcto */
                    translate = oddData.name.trim();
                break;
                case 17: // 0:0
                    translate = oddData.name.trim().replace(":","-");
                break;
            };

            /* Para las cuotas donde trae el participante en el marcador */
            if( score.length > 0 )
            {
                /* Si es empate, no buscamos el participante */
                if( score[0] == "draw" || score[0] == "empate" || score[0] == "drawn")
                    translate = score[1];   // Asignamos el marcador
                else
                {
                    /* Buscamos el participante */
                    var participantFind = _.find( participants, function (participant)
                    {
                        return participant.participant_feed.trim().toLowerCase() == score[0].toLowerCase();
                    });
                    /* Si hemos encontrado el participante */
                    if( participantFind != undefined )
                    {
                        /* Si el participante es el local */
                        if(participantFind.participant.trim().toLowerCase() == participantData.home.participant.toLowerCase() )
                            translate = score[1];
                        /* Si el participante es el visitante, invertimos el marcador */
                        else
                        {
                            /* goldenpark trae bien los marcadores, no es necesario darles la vuelta */
                            if( data.idFeed == 8 )
                                translate = score[1];
                            else
                                translate = reverseString(score[1]);
                        }
                    }
                    /* Si no hemos encontrado el participante */
                    else
                    {
                        logger.warn("[WARNING] No se encontro el participante: " + score[0] + " al comparararlos en: " + data.nameEvent + " en el mercado MARCADOR CORRECTO" );
                        //logData.writeLog(logParticipants, idSport + "|" + idFeed + "|" + strName , logFeed);
                    }
                }
            }

            /* Girar las cuotas */
            if( reverse ) translate = reverseString(translate);

            /* Solo si hemos podido traducir las cuotas se guardan */
            if( oddData.name != "" && translate != ""  )
            {
                aResults.push({
                    id: oddData.id,
                    name: translate,
                    name_feed: oddData.name,
                    odd: oddData.odd,
                    placebetlink: oddData.placebetlink
               });
            }
        });

        return aResults;
    }
    catch( e )
    {
        logger.error("[ERROR] al realizar el mercado MARCADOR CORRECTO para el evento: " + data.nameEvent );
        logger.error( e );
        return aResults;
    }
};

/* MERCADO GOLES OVER /UNDER */
var mGoalsOverUnder = module.exports.mGoalsOverUnder =  function( data, bets, logger )
{
    /* PATRON DE BESGAM */
    /* P 0.5 (Mas o plus de 0.5 goles)*/
    /* M 0.5 (Menos o minus de 0.5 goles) */
    /* Solo se recogen valores de X.5 goles */

    /* Se recorren los resultados */
    try
    {
        /* Array de resultados */
        var aResults = [];
            
        /* Recorremos las apuestas */
        _.each( bets, function( oddData )
        {
            var translate = "";

            /* Se busca el patron de cada feed para ajustarlo a nuestro resultado */
            switch( data.idFeed )
            {
                /* PAF */
                case 2:
                /* Suertia */
                case 9:
                /* 888 */
                case 12:
                    if( oddData.name == 'Menos' )
                    {
                        translate = 'M ' + oddData._;
                        oddData.name = oddData.name + " " + oddData._;
                    }
                    else if( oddData.name == 'MÃ¡s de' )
                    {
                        translate = 'P ' + oddData._;
                        oddData.name = oddData.name + " " + oddData._;
                    }
                break;
                /* LUCKIA */
                case 17:
                    /* Luckia nos trae el formato igual que nuestro patron */
                    /* P 2.5 - M 2.5 */
                    translate = oddData.name;
                break;
            }

            /* Comprobar que cumplen nuestro patron */
            var pattern = /[M|P]\s(\d)\.5/;  
            if( translate.match(pattern) != null && translate != "" )
            {
                aResults.push({
                    id: oddData.id,
                    name: translate,
                    name_feed: oddData.name,
                    odd: oddData.odd,
                    placebetlink: oddData.placebetlink
               });
            }
            else
                logger.error("[ERROR] No se encontro el tipo de apuesta en el mercado GOLES OVER /UNDER para el evento: " + data.nameEvent);

        });

        return aResults;
    }
    catch( e )
    {
        logger.error("[ERROR] al realizar el mercado GOLES OVER / UNDER para el evento: " + data.nameEvent );
        logger.error( e );
        return aResults;
    }

};

/* MERCADOS VERDADERO / FALSO */
var mTrueFalse = module.exports.mTrueFalse =  function(data, bets, logger )
{
    /* Se recorren los resultados */
    try
    {
        /* Array de resultados */
        var aResults = [];
            
        /* Recorremos las apuestas */
        _.each( bets, function( oddData )
        {
            var translate = "";

            switch( utf8.decode(String(oddData.name)).toLowerCase() )
            {
                case "si":
                    translate = "Si";
                break;
                case "sí":
                    translate = "Si";
                break;
                case "yes":
                    translate = "Si";
                break;
                case "no":
                    translate = "No";
                break;
                default:
                    logger.error("[ERROR] No se encontro el tipo de apuesta en  el mercado VERDADERO / FALSO para el evento: " + data.nameEvent );
                break;
            }

            if( translate != "" )
            {
                aResults.push({
                    id: oddData.id,
                    name: translate,
                    name_feed: oddData.name,
                    odd: oddData.odd,
                    placebetlink: oddData.placebetlink
               });
            }   
        });

        return aResults;

    }
    catch( e )
    {
        logger.error("[ERROR] al realizar el mercado VERDADERO / FALSO para el evento: " + data.nameEvent );
        logger.error( e );
        return aResults;
    }
};

/* MERCADO HANDICAP */
var mEuropeanHandicap = module.exports.mEuropeanHandicap =  function( data, bets, participantData, participants, fMarket, reverse, logger )
{
    /* PATRON DE BESGAM */
    /* Handicap +1 -> el local gana con mas de 1 gol de diferencia sobre el visitante 
       +1/1, +1/x,-1/2 */
    /* Handicap -1 -> el visitante gana con mas de 1 gol de diferencia sobre el local 
       -1/1, -1/x,+1/2 */
    /* El signo del empate  es siempre el mismo que el del local */

    /* Se recorren los resultados */
    try
    {
        /* Array de resultados */
        var aResults = [];
            
        /* Recorremos las apuestas */
        _.each( bets, function( oddData )
        {
            var translate = "";

            /* Se busca el patron de cada feed para ajustarlo a nuestro resultado */
            switch( data.idFeed )
            {
                /* PAF */
                case 2:
                /* Suertia */
                case 9:
                /* 888 */
                case 12:
                    /* Nos trae el participante por un lado y el handicap por otro */
                    var sing = ( oddData._ < 0 ) ? "" : "+";
                    translate = sing + oddData._ + "/" + oddData.name;
                break;
                /* LUCKIA */
                case 17:
                    /* -3/1 -3/x +3/2 */
                    /* Luckia nos trae el patron correcto */
                    translate = oddData.name;
                break;
            };

            /* Girar las cuotas */
            if(reverse)
            {
                if(translate.indexOf("+") >= 0)
                    translate.replace("+","-");
                else
                    translate.replace("-","+");

                if(translate.indexOf("/1") >= 0)
                    translate.replace("/1","/2");
                else
                    translate.replace("/2","/1");
            }

            /* Patron de las cuotas. Si no lo sigue, no lo hemos traducido bien y no se tiene en cuenta.*/
            var regPattern = /[\+|-](\d+)\/[(\d+)|x]/;

            if( translate != "" && regPattern.test(translate) )
            {
                aResults.push({
                    id: oddData.id,
                    name: translate,
                    name_feed: oddData.name,
                    odd: oddData.odd,
                    placebetlink: oddData.placebetlink
               });
            }   
        });

        return aResults;
    }
    catch( e )
    {
        logger.error("[ERROR] al realizar el mercado HANDICAP para el evento: " + data.nameEvent );
        logger.error( e );
        return aResults;
    }
};

/* MERCADO HANDICAP */
var mEuropeanHandicapBasket = module.exports.mEuropeanHandicapBasket =  function( data, bets, participantData, participants, fMarket, reverse, logger )
{
    /*********************************/
    /*      PATRON DE BESGAM         */
    /* -26.5/1 -> CSKA Moscow -26.5  */
    /* +26.5/2 -> Crvena Zvezda 26.5 */
    /*********************************/
    /* -26.5/1, +26.5/2              */
    /*********************************/

    /* Se recorren los resultados */
    try
    {
        /* Array de resultados */
        var aResults = [];
            
        /* Recorremos las apuestas */
        _.each( bets, function( oddData )
        {
            var translate = "";

            /* Se busca el patron de cada feed para ajustarlo a nuestro resultado */
            switch( data.idFeed )
            {
                /* PAF */
                case 2:
                    /* PAF nos trae el participante por un lado y el handicap por otro */
                    /* { name: 'Bilbao Basket',  _: -5.5, ... } */
                    var participant = getParticipant( participants, oddData.name );
                    if( participant != undefined )
                    {
                        var sing = ( oddData._ < 0 ) ? "" : "+";

                        if( participant.participant_feed == participantData.home.participant_feed )
                            translate = sing + oddData._ + "/1";
                        else if( participant.participant_feed == participantData.away.participant_feed )
                            translate = sing + oddData._ + "/2";
                    }
                break;
            };

            /* Girar las cuotas */
            if(reverse)
            {
                if(translate.indexOf("+") >= 0)
                    translate.replace("+","-");
                else
                    translate.replace("-","+");

                if(translate.indexOf("/1") >= 0)
                    translate.replace("/1","/2");
                else
                    translate.replace("/2","/1");
            }

            /* Patron de las cuotas. Si no lo sigue, no lo hemos traducido bien y no se tiene en cuenta.*/
            //var regPattern = /[\+|-](\d+)\/[(\d+)|x]/;

            if( translate != "" )
            {
                aResults.push({
                    id: oddData.id,
                    name: translate,
                    name_feed: oddData.name,
                    odd: oddData.odd,
                    placebetlink: oddData.placebetlink
               });
            }   
        });

        return aResults;
    }
    catch( e )
    {
        logger.error("[ERROR] al realizar el mercado HANDICAP BASKET para el evento: " + data.nameEvent );
        logger.error( e );
        return aResults;
    }
};

/* Buscamos un participante */
function getParticipant( aParticipants, find )
{
    return _.find( aParticipants, function( participant )
    {
        return participant.participant_feed.toLowerCase().trim() == find.toLowerCase().trim();
    });
};

/* Funciones generales */
function reverseString( string )
{
    /* Inicializamos */
    var result = "";
    /* Recorremos todos los caracters y ponemor del ultimo al inicio */
    for(var index = string.length -1; index >= 0; index--)
        result += string.charAt(index);
    /* Devolvemos el resultado */
    return result;
}