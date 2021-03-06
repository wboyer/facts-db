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

function addCorsHeaders (res)
{
	res.header('Access-Control-Allow-Origin', '*');
}

exports.findById = function(req, res)
{
	addCorsHeaders(res);

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
	addCorsHeaders(res);

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
	addCorsHeaders(res);

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
	addCorsHeaders(res);

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
	addCorsHeaders(res);

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

function summarizeTuples(tuples, start, givenNodes, nodes)
{
	var node = nodes[0];
	var map = [];
	var value;

	for (var i in tuples) {
		var tuple = tuples[i].tuple;
		value = tuple[node];

		if (value) {
			var entry = map[value];
			if (!entry) {
				entry = {};
				entry.value = value;
				entry.tuples = [];
				map[value] = entry;
			}
			if (Object.keys(tuple).length === (givenNodes.length + 1))
				entry.count = tuples[i].count;
			else
				entry.tuples.push(tuples[i]);
		}
	}

	var values = [];
	for (var key in map)
		values.push(map[key]);

	values.sort(function(a, b) {
		if (a.count < b.count)
			return 1;
		if (a.count > b.count)
			return -1;
		return 0;
	});

	if (start)
		values = values.slice(start);

	var result = { values: [], meta: {}}; 

	if (values.length > 10) {
		result.meta.more = values.length - 10;
		values = values.slice(0, 10);
	}
	else
		result.meta.more = 0;

	givenNodes.push(node);
	nodes = nodes.slice(1);

	for (key in values) {
		value = {};
		value.label = values[key].value;

		if (nodes.length) {
			var children = summarizeTuples(values[key].tuples, 0, givenNodes, nodes);
			if (children)
				value.children = children;
		}

		result.values.push(value);
	}

	if (!result.values.length)
		return null;

	var wrappedResult = {};
	wrappedResult[node] = result;

	return wrappedResult;
}

function searchByPrefix(node, q, start, res, func)
{
	db.collection('facts_summary', function(err, collection) {
		var query = JSON.parse('{"_id": { "' + node + '-pfx": "' + q + '"}}');
		collection.findOne(query, function(err, doc) {
			if (err) {
				console.error(err);
				res.statusCode = 503;
				res.send({'error': 'An error has occurred'});
			}
			else
				func(summarizeTuples(doc ? doc.value.tuples : [], start, [], [node]));
		});
	});
}

exports.search = function(req, res)
{
	addCorsHeaders(res);

	searchByPrefix(req.params.node, req.query.q, req.query.start, res, function(results) {
			res.send(results ? results : {});
	});
};
 
function searchByValue(node, q, start, res, func)
{
	db.collection('facts_summary', function(err, collection) {
		var query = JSON.parse('{"_id": { "' + node + '": "' + q + '"}}');
		collection.findOne(query, function(err, doc) {
			if (err) {
				console.error(err);
				res.statusCode = 503;
				res.send({'error': 'An error has occurred'});
			}
			else
				func(doc ? doc.value.tuples : []);
		});
	});
}

exports.browse = function(req, res)
{
	addCorsHeaders(res);

	var node = 'subj';
	if (req.params.node === 'obj')
		node = 'obj';

	var otherNode = (node === 'subj') ? 'obj' : 'subj';

	searchByPrefix(node, req.query.q, req.query.start, res, function(results) {
		if (!results) {
			res.send({});
			return;
		}

		results[otherNode] = {};
		results[otherNode].values = [];

		for (var i in results[node].values) {
			var otherNodeValue = {};
			otherNodeValue.label = results[node].values[i].label;
			results[otherNode].values.push(otherNodeValue);
		}

		function runQueryInSeries(node, i, nodes, final) {
			if (i == results[node].values.length) {
				if (final)
					final();
				return;
			}

			var value = results[node].values[i];

			searchByValue(node, value.label, 0, res, function(tuples) {
				var children = summarizeTuples(tuples, 0, [], nodes);
				if (children)
					value.children = children;
				runQueryInSeries(node, ++i, nodes, final);
			});
		}

		runQueryInSeries(node, 0, ['pred', otherNode], function() {
			runQueryInSeries(otherNode, 0, ['pred', node], function() {
				res.send(results);
			});
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
	app.get('/facts/browse/:node', exports.browse);
};

