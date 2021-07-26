const { createPool, createConnection } = require( 'mysql2/promise' );
const cpus = require( 'os' ).cpus().length

let clientOpts = {
    host: process.env.DB_HOST || 'localhost',
    user: 'benchmarkdbuser',
    password: 'benchmarkdbpass',
    database: 'hello_world'
};

let pool

const query = async ( text, values ) => ( await pool.query( text, values || undefined ) )[0]

module.exports = {
    async init() {
        const client = await createConnection( clientOpts )
        const res = await client.query( 'SHOW VARIABLES LIKE "max_connections"' )
        let maxConnections = Math.floor( res[0][0].Value * 0.9 / cpus )
        //1 worker per cpu, each worker pool gets a fraction of the max connections
        //only use 90% to avoid too many clients errors
        pool = createPool( Object.assign( { ...clientOpts }, { max: maxConnections } ) )
        await client.end()
    },
    allFortunes: async () =>
        query( 'SELECT * FROM fortune' ),

    worldById: async ( id ) =>
        query( `SELECT *
                FROM world
                WHERE id = ?`, [id] )
            .then( arr => arr[0] ),

    allWorlds: async () =>
        query( 'SELECT * FROM world' ),

    bulkUpdateWorld: async worlds => {
        let args = []
        for( let world of worlds ) {
            args.push( world.id, world.randomnumber )
        }
        return query(
            `INSERT INTO world (id, randomnumber)
                           VALUES ${worlds.map( () => `(?,?)` ).join( ',' )}
                  ON DUPLICATE KEY UPDATE randomnumber = VALUES(randomnumber)`,
            args )
            .then( () => worlds )
    }
}