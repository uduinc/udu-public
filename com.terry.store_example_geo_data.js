exports = module.exports = {
	id : 'com.terry.store_example_geo_data', //Must be same as file name
	input : [ 'gcs_filename', 'do_example_geo_parse' ], //Required inputs for n-app to run
	output : [ 'example_geo_parse_finished' ], //Outputs from n-app if it is successful
	input_optional : [],
	output_optional : [],
	do_not_cache : true, //Used primarily for testing when you want the n-app to always run (instead of pulling cached results) even with the same inputs
	meta : { //Meta for easier documentation/finding. Not required
		tags : [ 'utility', 'example' ],
		author : 'terry',
		documentation :
		{
			gcs_filename : 
			{
				description : "Name of the file uploaded to secure file upload",
				type : "string",
				example : "someCSVFile.csv"
			},
			do_example_geo_parse :
			{
				description : "Flag to parse the given CSV and store it",
				type : "boolean",
				example : true
			},
			example_geo_parse_finished : 
			{
				description : "Flag to indicate we are finished parsing and storing the data",
				type : "boolean",
				example : true
			}
		}
	},
	process : function ( req )
	{
		var self = this;

		var options = { //Options for CSV parser
			data_has_headers: true //First row of the csv is a header row
		};

		var dsOptions = { //Options for datasource
			dbType: "mongo" //Force it to store in mongo instead of elasticsearch (only relevant on first store, but safe to always include)
		};

		//Begin throttling declarations
		var stream = null;			
		options.streamPausing = function (s) {
			stream = s;
		}
		var isPaused = false;
		var numToProcess = 0;
		var numProcessed = 0;
		//End throttling declarations

		var parseErr = null; //var to keep track of any errors that may happen

		self.data.startBatchOperation( function ( ) { //Start a batch operation, define function to run when CSV parsing is done (all rows done or errored)
			if (parseErr) {
				console.error(parseErr);
				return self.fail(); //Did not successfully parse entire CSV, fail
			}
			else {
				return self.mutate({ //Parsed entire CSV, mutate flag to indicate success
					example_geo_parse_finished: true
				});
			}
		});

		//Note: The below function wipes all entries (but leaves indexes) of the datasource in question. Used when you want to start fresh every run so there are no duplicates
		self.data.clear("geo_data", dsOptions, function(err) {
			if (err) {
				parseErr = err;
				return self.data.finishBatchOperation();
			}

			self.storage.parseBucketSecureCSV(
				req.gcs_filename,
				',', //delimiter
				function onParse ( data ) {
					//Begin custom translations
					var output = {
						name: data.Name,
						geo_object: {
							type: "Polygon",
							coordinates: [[]]
						},
						rating: data.Rating
					};
					var coords = data.Coordinates.split(";"); //Coordinate pairs are separated by a ;
					for (var i=0; i<coords.length; i++) {
						var coordinatePair = coords[i].split(" "); //Each pair is separated by a space
						//Comes in as string, need to make it a number
						coordinatePair[0] = parseFloat(coordinatePair[0]);
						coordinatePair[1] = parseFloat(coordinatePair[1]);
						output.geo_object.coordinates[0].push(coordinatePair); //Example is in lon/lat order, which is what mongo needs, so push as-is
						//Below would be how we would do it if it was in lat/lon order
						//output.geo_object.coordinates[0].push([coordinatePair[1], coordinatePair[0]]);
						
					}

					//Begin throttling check
					numToProcess++;
					if (numToProcess - numProcessed > 1000 && stream && !isPaused) {
						//We have at least 1k async calls we're waiting on. throttle it
						isPaused = true;
						stream.pause();
					}
					//End throttling check
					self.data.add( "geo_data", output, dsOptions, function(err) {
						//Check to see if we've errored. We could end if we found an error, in this case we'll just keep going
						if (err) {
							parseErr = err;
						}

						//Begin throttling check
						numProcessed++;
						if (numToProcess - numProcessed < 100 && stream && isPaused) {
							//Only 100 or fewer calls we're waiting on, turn the stream back on
							stream.resume();
							isPaused = false;
						}
						//End throttling check
					});
				},
				function done ( err ) {
					if ( err ) {
						parseErr = err;
					}
					self.data.finishBatchOperation( );
				},
				options
			);
		});
	}
}