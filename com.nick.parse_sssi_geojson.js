exports = module.exports = {
	id : 'com.nick.parse_sssi_geojson',
	input : [ 'gcs_filename', 'datasource_name', 'parse_sssi_geojson' ],
	output : [ 'sssi_geojson_parse_finished' ],
	input_optional : [],
	output_optional : [],
	do_not_cache : true,
	meta : {
		tags : [ 'utility', 'datasources', 'geojson' ],
		author : 'nick',
		documentation :
		{
			gcs_filename : 
			{
				description : "Name of the file uploaded to secure file upload",
				type : "string",
				example : "someCSVFile.geojson"
			}
		}
	},
	process : function ( req )
	{
		let parseError = null;
		this.data.startBatchOperation( () => {
			if ( parseError ) {
				console.error( parseError );
				return this.fail( );
			}

			this.mutate( { sssi_geojson_parse_finished: true } );
		});

		console.log( 'Getting file contents' );

		this.storage.getSecureContents( req.gcs_filename, ( err, contents ) => {
			let data;
			try {
				data = JSON.parse( contents );
			} catch ( e ) {
				console.error( 'Error getting contents of geojson file:', err );
				return this.fail( );
			}

			console.log( 'Clearing old data' );

			this.data.clear( req.datasource_name, { dbType: 'mongo' }, err => {
				if ( err ) {
					console.error( 'Error clearing existing data from datasource:', err );
					return this.fail( );
				}

				const len = data.features.length;

				console.log( `Inserting ${len} documents` );

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
									console.log( 'done' );
									this.data.finishBatchOperation( );
								} else {
									console.log( `[ ${stopIdx} / ${len} ]` );
									setImmediate( insertGroup, stopIdx );
								}
							}
						});
					}
				};

				insertGroup( 0 );
			});
		});
	}
}