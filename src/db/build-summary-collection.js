db.runCommand(
	{
		mapReduce: "facts",

		map: function() { emit( "count", 1 ); },

		reduce: function(key, values) {
			return Array.sum( values );
		},

		out: { merge: "facts_summary" }
	});

db.runCommand(
	{
		mapReduce: "facts",

		map: function() {
			emit( { "subj": this.subj },
					{ "tuples": [ { "tuple": { "pred": this.pred }, "count": 1 },
								{ "tuple": { "pred": this.pred, "obj": this.obj }, "count": 1 } ] } );

			emit( { "pred": this.pred },
					{ "tuples": [ { "tuple": { "subj": this.subj }, "count": 1 },
								{ "tuple": { "obj": this.obj }, "count": 1 },
								{ "tuple": { "subj": this.subj, "obj": this.obj }, "count": 1 } ] } );

			emit( { "obj": this.obj },
					{ "tuples": [ { "tuple": { "pred": this.pred }, "count": 1 },
								{ "tuple": { "subj": this.subj, "pred": this.pred }, "count": 1 } ] } );

			function emitPrefixes(str, name) {
				var substrings = [];
				var regexp = /\w+/g;
				var word;
				var s;

				for (word = regexp.exec(str); word; word = regexp.exec(str)) {
					for (s in substrings)
						substrings[s] += ' ' + word[0];
					substrings.push(word[0]);
				}

				for (s in substrings)
					for ( var i = 1; (i < 10) && (i <= substrings[s].length); i++ ) {
						var key = JSON.parse('{"' + name + '-pfx" : "' + substrings[s].substring(0, i).toLowerCase() + '"}');
						emit( key,
									{ "tuples": [ { "tuple": { name: str }, "count": 1 } ] } );
					}
			}

			emitPrefixes(this.subj, "subj");
			emitPrefixes(this.pred, "pred");
			emitPrefixes(this.obj, "obj");
		},

		reduce: function(key, values) {
			var tuples = [];
			var tuple;
			var i, j;

			for ( i = 0; i < values.length; i++ ) {
				var value = values[i];
				for ( j = 0; j < value.tuples.length; j++ )
					tuples.push( value.tuples[j] );
			}

			for ( i = 0; i < tuples.length; i++ )
				for ( j = i + 1; j < tuples.length; j++ )
					if ( JSON.stringify ( tuples[i].tuple ) == JSON.stringify ( tuples[j].tuple ) ) {
						tuples[i].count += tuples[j].count;
						tuples.splice( j, 1 );
						j--;
					}

			return { "tuples": tuples };
		},

		out: { merge: "facts_summary" }
	});


