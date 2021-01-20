exports = module.exports = {
	id : 'com.sbir.search',
	input : ["sbir_search"],
	output : ["sbir_grants"],
	input_optional : ["sbir_keywords", "sbir_agency", "sbir_ri", "sbir_company", "sbir_year", "num_results"],
	output_optional : [],
	meta : {
		tags : [ 'grants', 'sbir' ],
		author : 'terry',
		documentation :
		{
			'sbir_search' : {
				description: 'Flag to search the SBIR grant API',
				type: 'boolean',
				example: true
			},
			'sbir_grants' : {
				description: 'Array of SBIR grant objects',
				type: 'array'
			},
			'sbir_keywords' : {
				description: 'Keywords to search on the SBIR API (all words are OR\'d)',
				type: 'array',
				example: ["space", "laser"]
			},
			'sbir_agency' : {
				description: 'Agency to search on the SBIR API (must be one of the following: "DOD","HHS","NASA","NSF","DOE","USDA","EPA","DOC","ED","DOT", or "DHS"',
				type: 'string',
				example: "DOE"
			},
			'sbir_ri' : {
				description: 'Research Institute to search on the SBIR API',
				type: 'string',
				example: "UCLA"
			},
			'sbir_company' : {
				description: 'Company to search on the SBIR API',
				type: 'string',
				example: "Udu"
			},
			'sbir_year' : {
				description: 'Year filter for SBIR grant API search',
				type: "string",
				example: "2017"
			}
		}
	},
	process : function(req)
	{
		const self = this;
		const validAgencies = ["DOD","HHS","NASA","NSF","DOE","USDA","EPA","DOC","ED","DOT","DHS"];
		let keywords = "";
		let query = {};
		let allResults = [];

		if (!req.sbir_keywords && !req.sbir_agency && !req.sbir_ri && !req.sbir_company && !req.sbir_year){
			console.log("Failure in com.sbir.search: no search terms.");
			return this.fail();		
		}

		if (req.sbir_agency && (typeof req.sbir_agency !== "string" || validAgencies.indexOf(req.sbir_agency.toUpperCase()) == -1)){
			console.log("Failure in com.sbir.search: invalid agency.");
			return this.fail();
		}		
		if (req.sbir_keywords) {
			//Handle strings. If it's not a string, assume it's an array or fail
			if (typeof req.sbir_keywords == "string"){
				keywords = req.sbir_keywords;
			}
			else if (typeof req.sbir_keywords == "object"){
				var isFirst = true;
				for (var i=0; i<req.sbir_keywords.length; i++){
					if (isFirst){
						keywords = req.sbir_keywords[i];
						isFirst = false;
					}
					else {
						keywords += " " + req.sbir_keywords[i];
					}
				}
			}
			else {
				console.log("Failure in com.sbir.search: invalid keyword structure. Use a string or an array of strings.");
				return this.fail();
			}
		}

		if (req.sbir_keywords){
			query.keyword = keywords;
		}
		if (req.sbir_agency){
			query.agency = req.sbir_agency;
		}
		if (req.sbir_ri){
			query.ri = req.sbir_ri;
		}
		if (req.sbir_company){
			query.company = req.sbir_company;
		}
		if (req.sbir_year){
			query.year = req.sbir_year;
		}

		const getPageOfResults = function (start) {
			query.start = start;

			self.request({
				method: 'GET',
				url: 'https://www.sbir.gov/api/awards.json',
				qs: query,
				json: true
			}, 
			function(err, res, obj)
			{
				if (!err && res && res.statusCode == 200 && !_.isEmpty(obj))
				{
					//We get an error object for nothing found, even though it's a 200
					if (obj.hasOwnProperty('ERROR'))
					{
						if (obj.ERROR != 'No record found.') {
							//Immediately bail out if we don't recognize this error object
							console.warn("com.sbir.search error", obj);
							return self.fail();
						}
						//No results found. Either we hit the end of our pagination or we found nothing
						if (!allResults.length) {
							return self.fail();
						}
						else {
							return self.mutate({
								sbir_grants: allResults
							});
						}
					}
					allResults = allResults.concat(obj);
					//Trim and mutate if we are at or over our desired number of results
					if (req.num_results && allResults.length >= req.num_results) {
						allResults.splice(req.num_results);
						return self.mutate({
							sbir_grants: allResults
						});
					}
					//100 results indicates potentially another page of results to fetch
					if (obj.length == 100) {
						return getPageOfResults(start+100);
					}
					//Less than 100 means we should be done
					if (allResults.length) {
						return self.mutate({
							sbir_grants: allResults
						});
					}
					return self.fail();
				} else {
					//Bail out on any error even if we have partial results
					let statusCode = "";
					if (res && res.statusCode)
						statusCode = res.statusCode
					console.error("Error in com.sbir.search: statusCode=", statusCode, "err=",err)
					self.fail();
				}
			},
			{
				key : 'sbir_gov',
				maxCallTime : 2000
			});
		}

		getPageOfResults(0);
	}
}