var mongodb = require('mongodb');

var Server = mongodb.Server;
var Db = mongodb.Db;
var BSON = mongodb.BSONPure;
 
var server = new Server('localhost', 27017, {auto_reconnect: true});
db = new Db('test', server);
 
db.open(function(err, db)
{
	if (!err) {
		db.collection('facts', {strict:true}, function(err, collection) {
		if (err) {
			console.error(err);
			console.error("The 'facts' collection doesn't exist.");
		}
		});
	}
});

exports.findById = function(req, res)
{
	var id = req.params.id;
	db.collection('facts', function(err, collection) {
		collection.findOne({'_id':new BSON.ObjectID(id)}, function(err, doc) {
			if (err) {
				console.error(err);
				res.statusCode = 503;
				res.send({'error': 'An error has occurred'});
			}
			else if (!doc)
				res.statusCode = 404;
			else
				res.send(doc);
		});
	});
};
 
exports.findAll = function(req, res)
{
	db.collection('facts', function(err, collection) {
		collection.find().toArray(function(err, docs) {
			if (err) {
				console.error(err);
				res.statusCode = 503;
				res.send({'error': 'An error has occurred'});
			}
			else
				res.send(docs);
		});
	});
};
 
exports.add = function(req, res)
{
	var facts = req.body;
	db.collection('facts', function(err, collection) {
		collection.insert(facts, {safe:true}, function(err, result) {
			if (err) {
				console.error(err);
				res.statusCode = 503;
				res.send({'error': 'An error has occurred'});
			} else
				res.send(result[0]);
		});
	});
};
 
exports.update = function(req, res)
{
	var id = req.params.id;
	var fact = req.body;
	db.collection('facts', function(err, collection) {
		collection.update({'_id':new BSON.ObjectID(id)}, fact, {safe:true}, function(err, doc) {
			if (err) {
				console.error(err);
				res.statusCode = 503;
				res.send({'error': 'An error has occurred.'});
			}
			else if (!doc)
				res.statusCode = 404;
					res.send(fact);
		});
	});
};
 
exports.delete = function(req, res)
{
	var id = req.params.id;
	db.collection('facts', function(err, collection) {
		collection.remove({'_id':new BSON.ObjectID(id)}, {safe:true}, function(err, result) {
			if (err) {
				console.error(err);
				res.statusCode = 503;
				res.send({'error': 'An error has occurred.'});
			} else
					res.statusCode = 200;
		});
	});
};

exports.search = function(req, res)
{
	var node = req.params.node;
	var q = req.query.q;
	var start = req.query.start;
	db.collection('facts_summary', function(err, collection) {
		var query = JSON.parse('{"_id": { "' + node + '-pfx": "' + q + '"}}');
		collection.findOne(query, function(err, doc) {
			if (err) {
				console.error(err);
				res.statusCode = 503;
				res.send({'error': 'An error has occurred'});
			}
			else {
				var results = [];

				if (doc) {
					var tuples = doc.value.tuples;

					tuples.sort(function(a, b) {
						if (a.count < b.count)
								return 1;
						if (a.count > b.count)
								return -1;
						return 0;
					});

					if (start)
							tuples = tuples.slice(start);

					for (var tuple in tuples)
						results.push(tuples[tuple].tuple);
				}

				res.send(results);
			}
		});
	});
};
 
exports.addRoutes = function(app)
{
	app.get('/facts', exports.findAll);
	app.get('/facts/:id', exports.findById);
	app.post('/facts', exports.add);
	app.put('/facts/:id', exports.update);
	app.delete('/facts/:id', exports.delete);
	app.get('/facts/search/:node', exports.search);
};


