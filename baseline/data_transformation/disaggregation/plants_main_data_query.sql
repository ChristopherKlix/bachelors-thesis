SELECT
	plants.id,
	plants.name,
	plants.park,
	plants.see_number,
	plants.eeg_key,
	plants.eeg_number,
	plants.report_id,
	plants.rated_power,
	plants.commissioning_date,
	plants.country,
	plants.federal_state,
	plants.zip_code,
	plants.nominal_power,
	plants.feed_in_date,
	plants.longitude,
	plants.latitude,
	plants.elevation,
	plants.azimuth,
	plants.slope,
	plants.controller_name,
	plants.bank,
	plants.om,
	plants.reporting,
	plants.plant_janitza_id,

	plants.poi_id,
	pois.name AS poi_name,
	pois.sel_number AS poi_sel_number,
	portfolios.name AS portfolio_name,
	pois.janitza_id AS poi_janitza_id,

	plants.spv_id,
	plants.portfolio_id,

	pois.san_number AS poi_san_number,

	st.short_name AS station_type_short_name,

	go.id AS grid_operator_id,
	go.name AS grid_operator_name,
	go.snb_number AS grid_operator_snb_number,
	got.short_name AS grid_operator_type_name,
	spvs.short_name AS spv_short_name,
	spvs.name AS spv_name
FROM main_data_2 plants
LEFT JOIN portfolios
ON plants.portfolio_id = portfolios.id
LEFT JOIN pois
ON plants.poi_id = pois.id
LEFT JOIN station_types st
ON pois.station_type_id = st.id
LEFT JOIN grid_operators go
ON pois.grid_operator_id = go.id
LEFT JOIN grid_operator_types got
ON go.grid_operator_type_id = got.id
LEFT JOIN spvs_current spvs
ON plants.spv_id = spvs.id
ORDER BY name ASC






SELECT
	plants.id,
	plants.name,
	plants.park,
	plants.see_number,
	plants.eeg_key,
	plants.eeg_number,

	plants.nominal_power,
	plants.rated_power,

	plants.commissioning_date,
	plants.feed_in_date,
	plants.country,
	plants.federal_state,
	plants.zip_code,
	plants.longitude,
	plants.latitude,
	plants.elevation,
	plants.azimuth,
	plants.slope,

	-- plants.poi_id,
	pois.name AS poi_name,
	pois.sel_number AS poi_sel_number,
	pois.san_number AS poi_san_number,
	-- pois.janitza_id AS poi_janitza_id,
	janitzas.name AS janitza_name,
	st.short_name AS station_type_short_name,

	-- go.id AS grid_operator_id,
	go.name AS grid_operator_name,
	go.snb_number AS grid_operator_snb_number,
	got.short_name AS grid_operator_type_name,

	plants.spv_id,
	spvs.short_name AS spv_short_name,
	spvs.name AS spv_name

FROM main_data_2 plants

LEFT JOIN pois
	ON plants.poi_id = pois.id
LEFT JOIN janitzas
    ON pois.janitza_id = janitzas.id
LEFT JOIN station_types st
	ON pois.station_type_id = st.id
LEFT JOIN grid_operators go
	ON pois.grid_operator_id = go.id
LEFT JOIN grid_operator_types got
	ON go.grid_operator_type_id = got.id
LEFT JOIN spvs_current spvs
	ON plants.spv_id = spvs.id

ORDER BY name ASC