exports = module.exports = {
	id : 'gov.wakeco.store_real_estate_records',
	input : [ 'gcs_filename', 'do_wakeco_store' ],
	output : [ 'wakeco_property_saved' ],
	input_optional : [ ],
	output_optional : [ ],
	meta : {
		tags : [ 'csv', 'gcs', 'wakeco' ],
		author : 'terry',
		documentation: {
			gcs_filename: {
				description: 'Name and path of the file on GCS',
				type: 'string'
			},
			do_wakeco_store: {
				description: 'Flag to keep us from entering here when we do not want to',
				type: 'boolean',
				example: true
			},
			wakeco_property_saved: {
				description: 'Flag indicating we are successful',
				type: 'boolean',
				example: true
			}
		}
	},
	process : function ( req )
	{

		var options = {
			data_has_headers: false
		};

		var clientObj = {};
		var self = this;
		var bathLookup = {
			A: "1 Bath",
			B: "1.5 Bath",
			C: "2 Bath",
			D: "2.5 Bath",
			E: "3 Bath",
			F: "3.5 Bath",
			G: "Limited Plumbing",
			H: "No Plumbing",
			I: "Adequate",
			J: "Number of Fixtures"
		};
		var typeLookup = {
			0: "Not Listed",
			1: "1 Family",
			2: "2 Family",
			3: "3 Family",
			4: "4 Family",
			5: "Multi-family",
			6: "Residence with Business Use",
			7: "Garden Apartments",
			8: "Townhouse Apartments",
			9: "Elevator Apartments",
			10: "Rooming House Apartments"
		};

//Subtract all starting points by 1 due to base 0				
// 1 35 Owner 1 488 4 Year of Addition
// 36 35 Owner 2 492 4 Effective Year
// 71 35 Mailing Address 1 496 4 Remodeled Year
// 106 35 Mailing Address 2 500 2 Unused
// 141 35 Mailing Address 3 502 8 Special Write In
// 176 7 Real Estate ID (REID) 510 1 Story Height
// 183 3 Card Number 511 1 Design Style
// 186 3 Number of Cards 512 1 Foundation Basement
// 189 6 Street Number 513 2 Foundation Basement %
// 195 2 Street Prefix 515 1 Exterior Wall
// 197 25 Street Name 516 1 Common Wall
// 222 4 Street Type 517 1 Roof
// 226 2 Street Suffix 518 1 Roof Floor System
// 228 2 Planning Jurisdiction 519 1 Floor Finish
// 230 5 Street Misc 520 1 Interior Finish
// 235 2 Township 521 1 Interior Finish 1
// 237 2 Fire District 522 2 Interior Finish 1 - %
// 239 12 Land Sale Price 524 1 Interior Finish 2
// 251 10 Land Sale Date (yyyy/mm/dd) 525 2 Interior Finish 2 - %
// 261 5 Zoning 527 1 Heat
// 266 8 Deeded Acreage 528 2 Heat %
// 274 11 Total Sale Price 530 1 Air
// 285 10 Total Sale Date (yyyy/mm/dd) 531 2 Air %
// 295 11 Assessed Building Value 533 1 Bath
// 306 11 Assessed Land Value 534 3 Bath Fixtures
// 317 19 Parcel Identification 537 15 Built In 1 Description
// 336 3 Special District 1 552 15 Built In 2 Description
// 339 3 Special District 2 567 15 Built In 3 Description
// 342 3 Special District 3 582 15 Built In 4 Description
// 345 1 Billing Class 597 15 Built In 5 Description
// 346 40 Property Description 612 3 City
// 386 1 Land Classification 615 5 Grade
// 387 6 Deed Book 620 3 Assessed Grade Difference
// 393 6 Deed Page 623 3 Accrued Assessed Condition %
// 399 10 Deed Date (yyyy/mm/dd) 626 1 Land Deferred Code
// 409 7 VCS (Neighborhood #) 627 9 Land Deferred Amount
// 416 40 Property Index 636 1 Historic Deferred Code
// 456 4 Year Built 637 9 Historic Deferred Amount
// 460 6 # of Rooms 646 6 Recycled Units
// 466 6 Units 652 1 Disqualifying & Qualifying Flags **
// 472 11 Heated Area 653 1 Land Disqualify & Qualify Flag **
// 483 3 Utilities 654 3 Type & Use
// 486 1 Street Pavement 657 50 Physical City
// 487 1 Topography 707 5 Physical Zip Code 


		var dsOptions = {
			uniqueID: "full_address",
			upsert: true
		};

		var stream = null;			
		options.streamPausing = function (s) {
			stream = s;
		}
		var isPaused = false;
		var numToProcess = 0;
		var numProcessed = 0;
		var parseErr = null;

		self.data.startBatchOperation( function ( ) {
			if (parseErr) {
				console.error(parseErr);
				return self.fail();
			}
			else {
				return self.mutate({wakeco_property_saved: true});
			}
		});


		this.storage.parseBucketCSV(
			req.gcs_filename,
			"!@#$",
			function onParse ( obj ) {
				var data = obj.field1;
				var zip = data.substr(706,5);
				var type = +data.substr(653,3);
				var companyNames = [];
				var peopleNames = [];
				var name = data.substr(0,35);
				if ( !/(LLC\.?|P\.?A\.?|Inc\.?)$/i.test( name ) && name.indexOf(",") != -1) {
					//Person's name (need first and last separated)
					name = name.split(",");
					var firstNames = name[1].split("&");
					for (var i=0; i<firstNames.length; i++) {
						//Handle things like JR, SR, II, III, and IV
						var f = firstNames[i].trim();
						var l = name[0].trim();
						var len = f.length;
						if (f.indexOf("JR") == len-2) {
							f = f.substring(0,f.indexOf("JR")).trim();
							l += " JR";
						}
						else if (f.indexOf("SR") == len-2) {
							f = f.substring(0,f.indexOf("SR")).trim();
							l += " SR";
						}
						else if (f.indexOf("II") == len-2) {
							f = f.substring(0,f.indexOf("II")).trim();
							l += " II";
						}
						else if (f.indexOf("III") == len-3) {
							f = f.substring(0,f.indexOf("III")).trim();
							l += " III";
						}
						else if (f.indexOf("IV") == len-2) {
							f = f.substring(0,f.indexOf("IV")).trim();
							l += " IV";
						}
						peopleNames.push({
							first_name: f,
							last_name: l
						});
					}
				} 
				else {
					//Company name
					companyNames.push(name.trim());
				}
				name = data.substr(35,35)
				if (name.trim()) {
					if ( !/(LLC\.?|P\.?A\.?|Inc\.?)$/i.test( name ) && name.indexOf(",") != -1) {
						//Person's name (need first and last separated)
						name = name.split(",");
						var firstNames = name[1].split("&");
						for (var i=0; i<firstNames.length; i++) {
							//Handle things like JR, SR, II, III, and IV
							var f = firstNames[i].trim();
							var l = name[0].trim();
							var len = f.length;
							if (f.indexOf("JR") == len-2) {
								f = f.substring(0,f.indexOf("JR")).trim();
								l += " JR";
							}
							else if (f.indexOf("SR") == len-2) {
								f = f.substring(0,f.indexOf("SR")).trim();
								l += " SR";
							}
							else if (f.indexOf("II") == len-2) {
								f = f.substring(0,f.indexOf("II")).trim();
								l += " II";
							}
							else if (f.indexOf("III") == len-3) {
								f = f.substring(0,f.indexOf("III")).trim();
								l += " III";
							}
							else if (f.indexOf("IV") == len-2) {
								f = f.substring(0,f.indexOf("IV")).trim();
								l += " IV";
							}
							peopleNames.push({
								first_name: f,
								last_name: l
							});
						}
					} 
					else {
						//Company name
						companyNames.push(name.trim());
					}
				}
				var purchasePrice = parseInt(data.substr(273,11));
				if (isNaN(purchasePrice)) {
					purchasePrice = "";
				}
				var buildingPrice = parseInt(data.substr(294,11));
				if (isNaN(buildingPrice)) {
					buildingPrice = "";
				}
				var landPrice = parseInt(data.substr(305,11));
				if (isNaN(landPrice)) {
					landPrice = "";
				}
				var totalPrice = "";
				if (buildingPrice || landPrice) {
					totalPrice = (isNaN(buildingPrice) ? 0 : buildingPrice) + (isNaN(landPrice) ? 0 : landPrice);
				}

				var output = {
					zip: zip,
					mail1: data.substr(70,35).trim(),
					mail2: data.substr(105,35).trim(),
					mail3: data.substr(140,35).trim(),
					street_num: data.substr(188,6).trim(),
					street_pre: data.substr(194,2).trim(),
					street_name: data.substr(196,25).trim(),
					street_type: data.substr(221,4).trim(),
					street_suffix: data.substr(225,2).trim(),
					purchase_price: purchasePrice,
					purchase_date: data.substr(284,10).trim(),
					land_assessed: landPrice,
					building_assessed: buildingPrice,
					total_assessed: totalPrice,
					city: data.substr(656,50).trim(),
					state: "NC",
					bath: bathLookup[data.substr(532,1)] || "Not Listed",
					type: typeLookup[type],
					type_code: type
				}
				if (!output.type) {
					output.type = "Not Listed"
				}
				if (!output.type_code) {
					delete output.type_code;
				}
				if (!output.purchase_date) {
					delete output.purchase_date;
				}
				if (output.street_num)
					output.street_num = parseInt(output.street_num);
				output.street_address = "";
				if (output.street_num)
					output.street_address += output.street_num + " ";
				if (output.street_pre) 
					output.street_address += output.street_pre + " ";
				if (output.street_name)
				 	output.street_address += output.street_name + " ";
				if (output.street_type)
					output.street_address += output.street_type + " ";
				if (output.street_suffix) 
					output.street_address += output.street_suffix;
				output.street_address = output.street_address.trim();

				output.full_address = output.street_address + " " + output.city + " " + output.state + " " + output.zip;
				if (companyNames.length) {
					output.raw_names = [companyNames[0]];
					output.owner_1 = companyNames[0];
					output.udu_is_company = true;
				}
				else {
					output.raw_names = []
					output.udu_is_company = false;
					for (var i=0; i<peopleNames.length; i++) {
						output.raw_names.push(peopleNames[i].first_name + " " + peopleNames[i].last_name);
						output["owner_"+(i+1)] = peopleNames[i].first_name + " " + peopleNames[i].last_name;
					}
				}
				numToProcess++;
				if (numToProcess - numProcessed > 1000 && stream && !isPaused) {
					//We have at least 1k async calls we're waiting on. throttle it
					isPaused = true;
					stream.pause();
				}
				self.data.update( "wakeco_property", {full_address: output.full_address} , output, dsOptions, function(err) {
					//Check to see if we've errored, or if this is the final callback so we can mutate
					if (err || numToProcess == -1) {
						if ( err ) {
							parseErr = err;
						}
					}
					numProcessed++;
					if (numToProcess - numProcessed < 100 && stream && isPaused) {
						//Only 100 or fewer calls we're waiting on, turn the stream back on
						stream.resume();
						isPaused = false;
					}
				});					
			},
			function done ( err ) {
				if ( err ) {
					numToProcess = -1;
				}
				self.data.finishBatchOperation( );
			},
			options
		);

	}
};