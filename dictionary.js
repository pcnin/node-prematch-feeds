/**********************************************************/
/*                      Includes                          */
/**********************************************************/
var DB 		= require('./bbdd/connection.js'),
	log4js  = require('log4js'),
	_		= require('underscore'),
	Q  		= require('q');
/**********************************************************/
/*                      Settings                          */
/**********************************************************/
log4js.configure({
  appenders: [
    { type: 'console' },
    //{ type: 'file', filename: 'logs/dictionary.log', category: 'dictionary' }
  ]
});

var logger 	= log4js.getLogger('dictionary'),
	timeout = 40000;
	//mysql 	= new DB();

/* Metodo que obtiene el diccionario de una feed */
var getDictionary = module.exports.getDictionary = function getDictionary(idFeed, mysql)
{
	var dictionary = {};

	return getLeagues( idFeed, mysql )
	.then( function(leagues)
	{
		dictionary.leagues = leagues;

		return getEvents( idFeed, mysql );
	})
	.then( function( events )
	{
		dictionary.events = events;

		return getParticipants( idFeed, mysql );
	})
	.then( function( participants )
	{
		dictionary.participants = participants;

		return getMarkets( idFeed, mysql )
	})
	.then( function( markets )
	{
		dictionary.markets = markets;

		return dictionary;
	})
	.catch( function(err)
	{
		return err;
	})	
};

function getLeagues( idFeed, mysql )
{
	var defer  	 = Q.defer(),
		aLeagues = [];

	mysql.query({
		sql: 'SELECT a.id_feed, a.id_league, b.id_sport, a.str_league, a.handle, a.explode, b.str_league as league_besgam FROM bg_league_feed a, bg_league b WHERE id_feed = ? AND a.id_league = b.id_league',
		timeout: timeout,
		values : [idFeed]
    })
	.then( function( res )
	{
		_.each( res, function ( elem )
		{
			/* Se guardan los datos */
	        if( typeof( aLeagues[elem.id_sport] ) == "undefined" )
	        	aLeagues[elem.id_sport] = [];

	        aLeagues[ elem.id_sport ].push({
	        	id_sport : elem.id_sport,
	        	handle: elem.handle,
	            id_feed: elem.id_feed,
	            id_league: elem.id_league,
	            str_league: elem.str_league || "",
	            league_besgam: elem.league_besgam || "",
	            explode: eval(elem.explode)
	        });
		});

		return defer.resolve( aLeagues );
	})
	.catch( function( err )
	{
		return defer.reject( err );
	});

	return defer.promise;
};

function getEvents( idFeed, mysql )
{
	var defer 	= Q.defer(),
		aEvents = [];

	mysql.query({
		sql: 'SELECT a.*, UNIX_TIMESTAMP(a.date_event) AS dateEvent, b.a_participant, c.id_sport FROM bg_event_feed a, bg_event b, bg_league c WHERE id_feed = ? AND a.id_event = b.id_event AND b.id_league = c.id_league',
		timeout: timeout,
		values: [idFeed]
	})
	.then( function (res)
	{
		_.each( res, function ( elem )
		{
			/* Se guardan los datos */
	        if( typeof( aEvents[elem.id_sport] ) == "undefined" )
	        	aEvents[elem.id_sport] = [];

	        aEvents[elem.id_sport].push({
	            id_sport : elem.id_sport,
	            id_league : elem.id_league,
	            id_event : elem.id_event,
	            id_event_feed : elem.id_event_feed,
	            str_event : elem.str_event,
	            date_event : new Date( elem.dateEvent * 1000),
	            date_update_event : elem.date_update_event,
	            handle : elem.handle,
	            a_participant : JSON.parse(elem.a_participant)
	        });

	        // console.log( new Date(elem.dateEvent * 1000) );
	        // console.log( elem.date_event );
	        // console.log("----------------")
		});

		return defer.resolve( aEvents );
	})
	.catch( function( err )
	{
		return defer.reject( err );
	});

	return defer.promise;
};

function getParticipants( idFeed, mysql )
{
	var defer  		  = Q.defer(),
		aParticipants = [];

	mysql.query({
		sql: 'SELECT b.id_participant, b.str_participant AS participant, a.str_participant AS participant_feed, a.id_sport FROM bg_participant_feed a, bg_participant b WHERE id_feed = ? AND b.id_participant = a.id_participant ORDER BY b.id_participant ASC ',
        timeout: timeout,
        values : [idFeed]
    })
	.then( function( res )
	{
		_.each( res, function ( elem )
		{
			/* Se guardan los datos */
	        if( typeof( aParticipants[elem.id_sport] ) == "undefined" )
	        	aParticipants[elem.id_sport] = [];

	        aParticipants[elem.id_sport].push({
	            id_sport : elem.id_sport,
	            id_participant : elem.id_participant,
	            participant_feed: elem.participant_feed || "",
	            participant: elem.participant || ""
	        });
		});

		return defer.resolve( aParticipants );
	})
	.catch( function( err )
	{
		return defer.reject( err );
	});

	return defer.promise;
};

function getMarkets( idFeed, mysql )
{
	var defer  	 = Q.defer(),
		aMarkets = [];

	mysql.query({
		sql: 'SELECT a.id_market, a.id_sport, a.id_group_market, a.str_in_market, a.marketType, b.str_market AS str_out_market FROM bg_market_language_feed a, bg_market b WHERE id_feed = ? AND a.id_market = b.id_market',
        timeout: timeout,
        values : [idFeed]
    })
	.then( function( res )
	{
		_.each( res, function ( elem )
		{
			/* Se guardan los datos */
	        if( typeof( aMarkets[elem.id_sport] ) == "undefined" )
	        	aMarkets[elem.id_sport] = [];

	        aMarkets[elem.id_sport].push({
            	id_market: elem.id_market,
            	id_sport:  elem.id_sport,
            	id_group_market: elem.id_group_market,
            	str_in_market: elem.str_in_market || "",
            	str_out_market: elem.str_out_market || "",
            	length_in_market: elem.str_in_market.length,
            	marketType: elem.marketType
          	});
		});

		return defer.resolve( aMarkets );
	})
	.catch( function( err )
	{
		return defer.reject( err );
	});

	return defer.promise;
};
