const { native } = require( 'pg' );
const genericPool = require( 'generic-pool' )
const cpus = require( 'os' ).cpus().length
const WORLD_BY_ID = 'world-by-id', ALL_WORLDS = 'all-worlds', ALL_FORTUNES = 'all-fortunes'
const prepareWorldStatement = i => `bulk-update-worlds-${i}`

const promisify = fn => async function() {
    return new Promise( ( resolve, reject ) => fn( ...arguments, ( err, reply ) => {
        if( err ) {
            console.log( err )
            reject( err )
        } else resolve( reply )
    } ) )
}

const pool = genericPool.createPool(
    {
        create: async () => {
            try {
                const client = new native.Client( {
                        host: 'tfb-database',
                        // host: 'host.docker.internal',
                        // host: 'localhost',
                        user: 'benchmarkdbuser',
                        password: 'benchmarkdbpass',
                        database: 'hello_world'
                    }
                )
                client.native.connect = promisify( client.native.connect.bind( client.native ) ).bind( client.native )
                client.native.execute = promisify( client.native.execute.bind( client.native ) ).bind( client.native )
                client.native.query = promisify( client.native.query.bind( client.native ) ).bind( client.native )
                client.native.prepare = promisify( client.native.prepare.bind( client.native ) ).bind( client.native )

                await client.connect()
                await client.native.prepare( WORLD_BY_ID, `SELECT *
                                                           FROM world
                                                           WHERE id = $1::int`, 1 )
                await client.native.prepare( ALL_FORTUNES, 'SELECT * FROM fortune', 0 )
                await client.native.prepare( ALL_WORLDS, 'SELECT * FROM world', 0 )
                for( let i = 1; i <= 500; i++ ) {
                    await client.native.prepare( prepareWorldStatement( i ),
                        `UPDATE world as w
                         SET randomnumber = wc.randomnumber
                         FROM (
                                  SELECT win.id, win.randomnumber
                                  FROM world wb,
                                       (VALUES ${
                                               //0 -> 1,2 ; 1 -> 3,4; 2 -> 5,6; 3 -> 7,8 ... = (i+1) * 2 - 1, (i+1) * 2
                                               Array.from( new Array( i ).keys() )
                                                       .map( i => ( i + 1 ) * 2 )
                                                       .map( i => `($${i - 1}::int,$${i}::int)` )
                                                       .join( ',' )
                                       }) AS win (id, randomnumber)
                                  WHERE wb.id = win.id
                                      FOR UPDATE
                              ) as wc
                         where w.id = wc.id`,
                        i * 2 )
                }
                return client.native
            } catch( e ) {
                console.error( e )
                throw e
            }
        },
        destroy: async client => await client.end()
    },
    {
        min: 2,
        //postgres max_connections = 2000
        //1 worker per cpu
        max: Math.floor( 2000 / cpus )
    }
)
const withClient = async fn => {
    let client = await pool.acquire()
    try {
        return await fn( client )
    } finally {
        await pool.release( client )
    }
}

let execute = async ( named, values ) => await withClient( async client => await client.execute( named, values || undefined ) )

module.exports = {
    randomId: () => Math.floor( Math.random() * 10000 ) + 1,
    randomUniqueIds: ( count ) => {
        const ids = {}
        for( let i = 0; i < count; i++ ) {
            let id = module.exports.randomId()
            if( ids[id] ) {
                for( let j = 0; j < 10000 - 1; j++ ) {
                    if( !ids[id] ) break
                    id++
                    if( id > 10000 ) {
                        id = 1
                    }
                }
            }
            ids[id] = true
        }
        return Object.keys( ids )
    },
    allFortunes: async () => await execute( ALL_FORTUNES ),
    worldById: async ( id ) => await execute( WORLD_BY_ID, [id] ).then( arr => arr[0] ),
    allWorlds: async () => await execute( ALL_WORLDS ),
    bulkUpdateWorld: async worlds => {
        const args = []
        for( let world of worlds ) {
            args.push( world.id, world.randomnumber )
        }
        return await execute( prepareWorldStatement( worlds.length ), args )
            .then( () => worlds )
    }
}
