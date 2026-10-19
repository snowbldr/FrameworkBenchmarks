const db = require('../db')
const { randomId } = require("../fn");
const { sjs, attr } = require('slow-json-stringify');

// Define serializers for different database casings, the verify tests fail without this
const mongoMysqlWorldObjectSerializer = sjs({ id: attr('number'), randomNumber: attr('number') });
const postgresWorldObjectSerializer = sjs({ id: attr('number'), randomnumber: attr('number') });
// Choose serializer based on the DATABASE environment variable
let serializer;
if (process.env.DATABASE === 'postgres') {
    serializer = postgresWorldObjectSerializer;
} else {
    // Default to camelCase for MongoDB and MySQL
    serializer = mongoMysqlWorldObjectSerializer;
}
const headers = { server: 'spliffy', 'content-type': 'application/json' };

module.exports = {
    GET: async () => {
        const row = await db.worldById(randomId());
        return { headers, body: serializer(row) };
    }
};
