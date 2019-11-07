exports = module.exports = {
	id: 'com.nick.parse_sssi_geojson',
	input: [ 'gcs_filename', 'datasource_name', 'parse_sssi_geojson' ],
	output: [ 'sssi_geojson_parse_finished' ],
	input_optional: [ 'sssi_do_not_overwrite' ],
	output_optional: [],
	do_not_cache: true,
	meta: {
		tags: [ 'utility', 'datasources', 'geojson' ],
		author: 'nick',
		documentation:
		{
			gcs_filename: {
				description: 'Name of the file uploaded to secure file upload',
				type: 'string',
				example: 'someGeoJSONFile.geojson'
			},
			sssi_do_not_overwrite: {
				description: 'Whether to skip deleting old data from the datasource before inserting new data',
				type: 'boolean',
				example: true
			}
		}
	},
	process: function ( req )
	{
		// make sure this goes to the appropriate org's datasource rather than global udu
		this.data.setOrganization( () => {} );

		let parseError = null;
		this.data.startBatchOperation( () => {
			if ( parseError ) {
				console.error( parseError );
				return this.fail( );
			}

			this.mutate( { sssi_geojson_parse_finished: true } );
		});

		// console.log( 'Getting file contents' );

		this.storage.getSecureContents( req.gcs_filename, ( err, contents ) => {
			let data;
			try {
				data = JSON.parse( contents );
			} catch ( e ) {
				console.error( 'Error getting contents of geojson file:', err );
				return this.fail( );
			}
			const len = data.features.length;

			// Insert in batches of 100
			// Attempting to insert all of the data at once could crash nodejs
			const insertGroup = startIdx => {
				const stopIdx = Math.min( startIdx+100, len );
				let waiting = stopIdx-startIdx;

				for ( let i=startIdx; i<stopIdx; i++ ) {
					this.data.add( req.datasource_name, data.features[ i ], { dbType: 'mongo' }, err => {
						if ( err ) {
							console.error( '<> got error on', i );
							parseError = err;
						}
						if ( !( --waiting ) ) {
							if ( stopIdx === len ) {
								// console.log( 'done' );
								this.data.finishBatchOperation( );
							} else {
								// console.log( `[ ${stopIdx} / ${len} ]` );
								setImmediate( insertGroup, stopIdx );
							}
						}
					});
				}
			};

			if ( req.sssi_do_not_overwrite ) {
				insertGroup( 0 );
			} else {
				// console.log( 'Clearing old data' );

				this.data.clear( req.datasource_name, { dbType: 'mongo' }, err => {
					if ( err ) {
						console.error( 'Error clearing existing data from datasource:', err );
						return this.fail( );
					}

					// console.log( `Inserting ${len} documents` );
					insertGroup( 0 );
				});
			}
		});
	}
}