SELECT DISTINCT id FROM users
WHERE id IN (
	SELECT DISTINCT users.id FROM users
	JOIN ride_user ON ride_user.user_id = users.id
	JOIN rides ON rides.id = ride_user.ride_id
	WHERE ride_user.status = 'driver'
	AND rides.deleted_at IS NULL
	AND rides.done = TRUE 
)
OR id IN (
	SELECT DISTINCT users.id FROM users
	JOIN ride_user ON ride_user.user_id = users.id
	JOIN rides ON rides.id = ride_user.ride_id
	WHERE ride_user.status = 'accepted'
	AND rides.deleted_at IS NULL
	AND rides.done = TRUE
)