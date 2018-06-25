exports = module.exports = {
	id : 'org.wikipedia.search',
	input : [ 'search_term' ],
	output : [ 'wikipedia_title', 'wikipedia_snippet' ],
	input_optional : [],
	output_optional : [],
	meta : {
		tags : [ 'search' ],
		author : 'rett',
		documentation :
		{
			search_term : {
				description: 'Search term',
				type: 'string',
				example: 'raleigh nc'
			},
			wikipedia_title: {
				description: 'A title of a wikipedia page',
				type : 'string'
			},
			wikipedia_snippet: {
				description: 'A snippet from a wikipedia page',
				type : 'string'
			}
		}
	},
	process : function( req )
	{
		var self = this;

		var params =
		{
			action : 'query',
			list : 'search',
			srsearch : req.search_term,
			prop : 'info',
			inprop : 'url',
			format : 'json'
		};

		this.request(
		{
			method: 'GET',
			url: 'https://en.wikipedia.org/w/api.php',
			qs: params,
			json: true
		}, 
		function( err, res, obj )
		{
			if ( ! err && res.statusCode == 200 )
			{
				console.log( self.id, '>>>', obj );
				var data = _.get( obj, 'query.search.0' );
				if ( data )
				{
					self.mutate(
					{
						wikipedia_title : data.title,
						wikipedia_snippet : data.snippet
					});
					return;
				}
			}
			self.fail();
		});
	}
}