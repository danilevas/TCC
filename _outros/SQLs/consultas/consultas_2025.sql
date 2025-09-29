--Ver os status de caronas dos usuários car_owner = false com qualquer dado do carro
SELECT users.id, name, car_owner, car_model, car_color, car_plate, ride_id, status FROM users
JOIN ride_user ON ride_user.user_id = users.id 
WHERE car_owner = false
AND (
	LENGTH(car_model) >= 1
	OR LENGTH(car_color) >= 1
	OR LENGTH(car_plate) >= 1
)
GROUP BY users.id, name, car_owner, car_model, car_color, car_plate, ride_id, status
ORDER BY users.id

-- Sobraram 7 registros de car_owner = false com algum dado do carro
-- 5 só tem a cor
-- 2 só tem uma placa, um com 3 letras e outro com 1 só
-- Nenhum deles participou do sistema de busca/oferta de caronas

-- Ver valores em users.location que não estão em neighborhoods
SELECT u.location, COUNT(*) AS contagem FROM users u
FULL JOIN neighborhoods n ON u.location = n.name
WHERE u.location NOT IN (
	SELECT name FROM neighborhoods
)
AND u.location IS NOT NULL
AND u.location != ''
GROUP BY u.location
ORDER BY contagem DESC

-- Ver cpfs que não tem 11 dígitos, ou que não são só números
SELECT id, name, id_ufrj FROM users
WHERE LENGTH(id_ufrj) NOT IN (11)
-- WHERE id_ufrj !~ '^[0-9]+$'

-- Basicamente aqui temos 43 códigos de gringos que tem entre 8 e 10 caracteres, números e letras,
-- todos começando com "E", e 1 CPF de um cara que só tem 10 dígitos

-- Caronas com done=True sem nenhum pedido com status accepted e seus pedidos
SELECT rides.id, rides.created_at, rides.updated_at, rides.date, ride_user.created_at, ride_user.updated_at,
ride_user.status FROM rides
LEFT JOIN ride_user ON rides.id = ride_user.ride_id
WHERE rides.done = TRUE
AND rides.id NOT IN (
	SELECT DISTINCT rides.id FROM rides
	JOIN ride_user ON rides.id = ride_user.ride_id
	WHERE rides.done = TRUE
	AND ride_user.status = 'accepted'
)
ORDER BY rides.id

-- Dessas, 23 tem pedidos e 34 não.

-- 5984 caronas com done=TRUE
-- 5950 caronas com done=TRUE com algum pedido
-- 5927 caronas com done=TRUE com algum pedido accepted

-- Das 23 caronas com pedidos, 19 tem algum pedido quit, ou seja, tiveram algum pedido accepted em algum momento. 4 só tem pedido driver. Temos 7 pedidos quit ocorridos após a hora em que a carona deveria acontecer (em teoria).

-- Podemos teorizar que essas 34 sem pedidos foram apagadas, e que essas 23 com pedidos

-- Auxílio
SELECT DISTINCT rides.id FROM rides
JOIN ride_user ON rides.id = ride_user.ride_id
WHERE rides.done = TRUE
AND ride_user.status = 'accepted'
ORDER BY rides.id

-- Caronas com done=True sem nenhum pedido com status accepted e seus pedidos
SELECT rides.id, rides.created_at, rides.updated_at, rides.date, ride_user.created_at, ride_user.updated_at,
ride_user.status FROM rides
JOIN ride_user ON rides.id = ride_user.ride_id
WHERE rides.done = TRUE
AND rides.id NOT IN (
	SELECT DISTINCT rides.id FROM rides
	JOIN ride_user ON rides.id = ride_user.ride_id
	WHERE rides.done = TRUE
	AND ride_user.status = 'accepted'
)
-- AND ride_user.status = 'quit'
-- AND ride_user.updated_at > rides.date
ORDER BY rides.created_at, ride_user.created_at