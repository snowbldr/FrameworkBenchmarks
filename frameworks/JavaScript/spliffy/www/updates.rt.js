const db = require('../db')
const { parseCount, randomId } = require("../fn");

const headers = { server: 'spliffy', 'content-type': 'application/json' };

module.exports = {
    GET: async ({ url }) => {
        const count = parseCount(url.query.queries);
        const results = [];
        for (let i = 0; i < count; i++) {
            results.push(db.worldById(randomId()));
        }
        const worlds = await Promise.all(results);
        for (let i = 0; i < count; i++) {
            worlds[i].randomNumber = randomId();
        }

        if (worlds.length === 0) {
            return { headers, body: JSON.stringify([]) };
        }

        await db.bulkUpdateWorld(worlds);
        return { headers, body: JSON.stringify(worlds) };
    }
}
