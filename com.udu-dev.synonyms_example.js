exports = module.exports = {
	id : 'com.udu-dev.synonyms_example',
	input : [ ],
	output : [  ],
	init : function ( ) {
		this._ = require( 'lodash' );
		this.synonyms = {
			"Company Name" : [ "corporation_name", "company_name" ],
			"First Name" : [ "first_name" ],
			"Last Name" : [ "last_name" ]
		};
		this.input_optional = _.keys( this.synonyms );
		this.output_optional = [];
		for ( var key in this.synonyms )
		{
			for ( var i = 0; i < this.synonyms[ key ].length; i++ )
			{
				this.output_optional.push( this.synonyms[ key ][ i ] );
			}
		}
	},
	process : function(req)
	{
		var _ = this._;
		var mut = {};
		for ( var key in this.synonyms )
		{
			if ( _.has( req, key ) )
			{
				for ( var i = 0; i < this.synonyms[ key ].length; i++ )
				{
					mut[ this.synonyms[ key ][ i ] ] = req[ key ];
				}
			}
		}

		this.mutate( mut );
	}
}
