/*
 ***** BEGIN LICENSE BLOCK *****
 
 This file is part of the Zotero Data Server.
 
 Copyright © 2017 Center for History and New Media
 George Mason University, Fairfax, Virginia, USA
 http://zotero.org
 
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.
 
 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 
 ***** END LICENSE BLOCK *****
 */

const mysql2 = require('mysql2');
const mysql2Promise = require('mysql2/promise');
const sqlite = require('sqlite');
const through2 = require('through2');
const request = require('request');
const config = require('./config');

let indexed = 0;
let indexedTotal = 0;
let currentShardID = 0;
let failedShards = 0;
let done = false;

async function getShardDate(db, shardID) {
	let row = await db.get('SELECT shardDate FROM shards WHERE shardID = ?', [shardID]);
	if (!row) return new Date(0).toISOString();
	return row.shardDate;
}

async function setShardDate(db, shardID, shardDate) {
	await db.run('INSERT OR REPLACE INTO shards (shardID, shardDate) VALUES (?,?)',
		[shardID, shardDate]);
}

function index(batch, callback) {
	if (!batch.length) return callback();
	request({
		url: config.indexerURL,
		method: 'POST',
		json: batch,
	}, function (err, res) {
		if (err) return callback(err);
		callback()
	});
}

function streamShard(connectionInfo, shardDateFrom) {
	return new Promise(function (resolve, reject) {
		let connection = mysql2.createConnection(connectionInfo);
		
		connection.connect(function (err) {
			if (err) return reject(err);
			
			let sql = `
				SELECT itmd1.value AS title,
				       itmd2.value AS doi,
				       itmd3.value AS isbn,
				       creators.lastName as name,
				       itm.serverDateModified AS shardDate
				FROM itemCreators, creators, items AS itm
				LEFT JOIN itemData AS itmd1 ON (itmd1.itemID = itm.itemID AND itmd1.fieldID IN (110,111,112,113))
				LEFT JOIN itemData AS itmd2 ON (itmd2.itemID = itm.itemID AND itmd2.fieldID=26 )
				LEFT JOIN itemData AS itmd3 ON (itmd3.itemID = itm.itemID AND itmd3.fieldID=11 )
				WHERE itm.serverDateModified >= ?
				AND itm.itemTypeID != 1
				AND itm.itemTypeID != 14
				AND itemCreators.itemID = itm.itemID
				AND itemCreators.orderindex = 0
				AND creators.creatorID = itemCreators.creatorID
				AND itmd1.value IS NOT NULL
				GROUP BY itm.itemID
			`;
			
			let shardDate = new Date(0).toISOString();
			let batch = [];
			
			connection.query(sql, [shardDateFrom])
				.stream({highWaterMark: 100000})
				.pipe(through2({objectMode: true}, function (row, enc, next) {
					
					if (row.shardDate.toISOString() > shardDate) {
						shardDate = row.shardDate.toISOString();
					}
					
					let identifiers = [];
					if (row.doi) identifiers.push(row.doi);
					if (row.isbn) identifiers.push(row.isbn);
					identifiers = identifiers.join(',');
					
					batch.push({
						title: row.title,
						name: row.name,
						identifiers: identifiers
					});
					
					if (batch.length >= 500) {
						index(batch, function (err) {
							connection.close();
							if (err) return reject(err);
							indexed += batch.length;
							batch = [];
							next();
						});
					}
					else {
						next();
					}
					
				}))
				.on('data', function () {
				})
				.on('end', function () {
					index(batch, function (err) {
						connection.close();
						if (err) return reject(err);
						indexed += batch.length;
						resolve(shardDate);
					});
				})
		});
	});
}

async function main() {
	console.time("total time");
	
	let db = await sqlite.open('./db.sqlite', {Promise});
	await db.run("CREATE TABLE IF NOT EXISTS shards (shardID INTEGER PRIMARY KEY, shardDate TEXT)");
	
	let master = await mysql2Promise.createConnection({
		host: config.masterHost,
		user: config.masterUser,
		password: config.masterPassword,
		database: config.masterDatabase
	});
	
	let [shardRows] = await master.execute(
		"SELECT * FROM shards AS s LEFT JOIN shardHosts AS sh USING(shardHostID) WHERE s.state = 'up' AND sh.state = 'up' ORDER BY shardID"
	);
	master.close();
	
	for (let i = 0; i < shardRows.length; i++) {
		let shardRow = shardRows[i];
		
		try {
			currentShardID = shardRow.shardID;
			let shardDate = await getShardDate(db, shardRow.shardID);
			let connectionInfo = {
				host: shardRow.address,
				user: config.masterUser,
				password: config.masterPassword,
				port: shardRow.port,
				database: shardRow.db
			};
			
			shardDate = await streamShard(connectionInfo, shardDate);
			await setShardDate(db, shardRow.shardID, shardDate);
		}
		catch (err) {
			failedShards++;
			console.log(err);
		}
	}
	
	await db.close();
	console.timeEnd("total time");
	done = true;
}

setInterval(function () {
	indexedTotal += indexed;
	console.log('current shard: ' + currentShardID + ', failed shards: ' + failedShards + ', indexed total: ' + indexedTotal + ', indexed per second: ' + Math.floor(indexed / 1));
	indexed = 0;
	if (done) {
		process.exit(0);
	}
}, 1000);

main();
