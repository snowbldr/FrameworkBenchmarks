const postgres = require( 'postgres' );

const sql =postgres({
    host: process.env.DB_HOST || 'localhost',
    user: 'benchmarkdbuser',
    password: 'benchmarkdbpass',
    database: 'hello_world',
    max: 1
});

module.exports = {
    async init() {},
    allFortunes: async () =>
        await sql`SELECT id, message FROM fortune`,

    worldById: async ( id ) =>
        await sql`SELECT id, randomNumber FROM world WHERE id = ${id}`.then( arr => arr[0] ),

    allWorlds: async () =>
        sql`SELECT * FROM world`,

    bulkUpdateWorld: async worlds => await  sql `UPDATE world SET randomnumber = (update_data.randomNumber)::int FROM (
                    VALUES ${sql( 
                        worlds.map(world => [world.id, world.randomNumber])
                                .sort((a, b) => (a[0] < b[0]) ? -1 : 1)
                    )}) AS update_data (id,randomNumber)
                    WHERE world.id = (update_data.id)::int
                `
}
