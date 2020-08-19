exports = module.exports = {
	id : 'com.rett.date_to_string',
	input : [ 'date_format' ], 
	output : [ 'date_string' ],
	input_optional : [ 'date_offset', 'specific_date' ],
	output_optional : [],
	meta : {
		tags : [ 'date', 'time' ],
		author : 'rett',
	},
	init : function ( )
	{
		this.moment = this.require( 'moment' );
	},
	do_not_cache: true,
	process : function( req )
	{
		var moment = this.moment;

		var dateString = 'ERROR';
		var date_offset = 0 || req.date_offset;
		if ( req.specific_date )
		{
			var d = new Date( req.specific_date );
			var sd = d.getFullYear() +  _.padStart( '' + ( d.getMonth() + 1 ), 2, '0' ) + _.padStart( '' + d.getDate(), 2, '0' );
			dateString = moment( sd, 'YYYYMMDD' ).format( req.date_format );
		} else {
			if ( date_offset == 0 )
			{
				dateString = moment().format( req.date_format );
			} else if ( date_offset > 0 ) {
				dateString = moment().add( date_offset, 'days' ).format( req.date_format );
			} else {
				dateString = moment().subtract( -date_offset, 'days' ).format( req.date_format );
			}
		}
		if ( dateString == 'ERROR' )
		{
			return this.fail();
		}
		var mut = 
		{
			date_string : dateString
		};

		this.mutate( mut );
	}
}