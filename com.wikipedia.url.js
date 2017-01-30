exports = module.exports = {
	id : 'com.wikipedia.url',
	input : ["search_term"],
	output : ["wikipedia_url"],
	input_optional : [],
	output_optional : [],
	meta : {
		tags : [ 'search' ],
		author : 'unknown',
		documentation :
		{
			search_term : {
				description: 'Search term',
				type: 'string',
				example: 'raleigh nc'
			},
			wikipedia_url: {
				description: 'A url to a wikipedia page',
				type : 'string'
			}
		}
	},
	process : function(req)
	{
		var _id = this.id;

		var params =
		{
			format : 'json',
			action : 'query',
			titles : req.search_term,
			prop : 'info',
			inprop : 'url',
			indexpageids : ''
		};

		this.request({
			method: 'GET',
			url: "http://en.wikipedia.org/w/api.php",
			qs: params,
			json: true
		}, 
		function(err, res, obj)
		{
			if (!err && res.statusCode == 200)
			{
				var thispage = obj.query.pageids[0];
				if ( Number( thispage ) >= 0 ) // dh : -1 indicates there is not yet a page with the returned url
				{
					var mut = 
					{
						source : _id,
						wikipedia_url : obj.query.pages[thispage].fullurl
					};
					udu.mutateRequest(req, mut);
				} else {
					udu.addFailureToRequest(req, _id);
				}
			} else {
				udu.addFailureToRequest(req, _id);
			}
		});
	}
}