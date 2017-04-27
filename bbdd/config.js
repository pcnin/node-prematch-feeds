const DB_CONFIG = {
    connectionLimit     : 100,
    acquireTimeout      : 60 * 60 * 1000,
    timeout             : 60 * 60 * 1000,
    waitForConnections  : true,
    queueLimit          :0,

    // host: '54.148.198.12',
    // user: 'besgampro',
    // password:'bg9T25x3a',
    // database:'besgam',
    // charset:'UTF8_GENERAL_CI',

    host: 'besgamdb.cbo5b66nlkyh.us-west-2.rds.amazonaws.com',
    user: 'besgamdb',
    password:'Apuesta10Futuro',
    database:'besgam',
    charset:'UTF8_GENERAL_CI'

    //debug:true
}
exports.DB_CONFIG = DB_CONFIG;