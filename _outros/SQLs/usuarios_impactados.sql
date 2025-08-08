SELECT DISTINCT user_sk FROM dim_user
WHERE user_sk IN (
	SELECT DISTINCT du.user_sk FROM dim_user du
	JOIN fact_ride_request frr ON frr.user_sk = du.user_sk
	JOIN fact_ride fr ON fr.ride_sk = frr.ride_sk
	JOIN dim_request_status drs ON drs.status_sk = frr.status_sk
	JOIN dim_ride_flags drf ON drf.ride_flags_sk = fr.ride_flags_sk
	WHERE drs.status_name = 'accepted'
	AND drf.deleted = FALSE
	AND drf.done = TRUE
)
OR user_sk IN (
	SELECT DISTINCT du.user_sk FROM dim_user du
	JOIN fact_ride fr ON fr.driver_user_sk = du.user_sk
	JOIN dim_ride_flags drf ON drf.ride_flags_sk = fr.ride_flags_sk
	AND fr.accepted_requests_count > 0
	AND drf.deleted = FALSE
	AND drf.done = TRUE
)