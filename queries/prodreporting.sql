select	format(date, 'yyyy-MM-dd') as date
		-- , fundingSource
		, channelName
		, subChannelName
		, hotelCode
		-- , CH.hotel_Name as hotelName
		-- , case when datename(dw, [date]) in ( 'Saturday', 'Sunday' ) then 'Weekend' else 'Weekday' end as weekend
		-- , isnull(json_value(hotelAttributes, '$.enterprise.strMarket'), '') as strMarket
		-- , isnull(json_value(hotelAttributes, '$.enterprise.market'), '') as market
		, isnull(json_value(hotelAttributes, '$.enterprise.brand'), '') as brand
		, sum(transactions) as bookings
		, sum(roomNights) as roomNights
		, sum(revenue) as revenue
		, sum(consumedRevenue) as consumedRevenue
from	Fact.AdvancedPropertyDash F with(nolock)
		inner join Dim.ClientHotel CH with(nolock)	on	CH.hotelKey = F.hotelKey
		inner join Dim.EnterpriseChannel EC with(nolock)	on	EC.enterpriseChannelId = F.enterpriseChannelId
															--and EC.siteCompanyGroupId = F.siteCompanyGroupId
		inner join Dim.EnterpriseSubChannel ESC with(nolock)	on	ESC.enterpriseSubChannelId = F.enterpriseSubChannelId
																--and ESC.enterpriseChannelId = EC.enterpriseChannelId
where	F.[date] between {} and {}
		and F.siteCompanyGroupId = {}
		and CH.countryCode in ( 'USA', 'CAN' )
group by	format(date, 'yyyy-MM-dd')
			-- , fundingSource
			, channelName
			, subChannelName
			, hotelCode
			-- , CH.hotel_Name
			-- , case when datename(dw, [date]) in ( 'Saturday', 'Sunday' ) then 'Weekend' else 'Weekday' end
			-- , isnull(json_value(hotelAttributes, '$.enterprise.strMarket'), '')
			-- , isnull(json_value(hotelAttributes, '$.enterprise.market'), '')
			, isnull(json_value(hotelAttributes, '$.enterprise.brand'), '')
