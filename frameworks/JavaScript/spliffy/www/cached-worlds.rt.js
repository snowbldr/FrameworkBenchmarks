const { parseCount, randomUniqueIds } = require("../fn");
const NodeCache = require( 'node-cache' )
const worldCache = new NodeCache()
const db = require('../db')
if(db.allWorlds){
    db.allWorlds().then( worlds => worlds.forEach( world => worldCache.set( world.id, world ) ) )
}
const headers = { server: 'spliffy', 'content-type': 'application/json' };

module.exports = {
    GET: async ({ url }) => {
        const count = parseCount(url.query.count);
        const ids = randomUniqueIds(count)
        const results = [];
        for (let i = 0; i < count; i++) {
            results.push(worldCache.get(ids[i]));
        }
        const worlds = await Promise.all(results);
        return { headers, body: JSON.stringify(worlds) };
    }
}
