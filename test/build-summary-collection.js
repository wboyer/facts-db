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

			var subj = this.subj;
			for ( var i = 1; i <= subj.length; i++ )
				emit( { "subj-pfx": subj.substring( 0, i ) },
					  { "tuples": [ { "tuple": { "subj": subj }, "count": 1 } ] } );
		},

		reduce: function(key, values) {
			var tuples = new Array();
			var tuple;

			for ( var i = 0; i < values.length; i++ ) {
				var value = values[i];
				for ( var j = 0; j < value.tuples.length; j++ )
					tuples.push( value.tuples[j] );
			}

			for ( var i = 0; i < tuples.length; i++ )
				for ( var j = i + 1; j < tuples.length; j++ )
					if ( JSON.stringify ( tuples[i].tuple ) == JSON.stringify ( tuples[j].tuple ) ) {
						tuples[i].count += tuples[j].count;
						tuples.splice( j, 1 );
						j--;
					}

			return { "tuples": tuples };
		},

		out: { merge: "facts_summary" }
	});
