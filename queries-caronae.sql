--Exportar como CSV:
\copy (SELECT * FROM tabela) TO ‘pasta/nome.csv’ DELIMITER ‘,’ CSV HEADER
\copy (SELECT * FROM tabela1 LEFT JOIN tabela2 ON tabela2.id = tabela1.tabela2_id) TO ‘pasta/nome.csv’ DELIMITER ‘,’ CSV HEADER

--Executar linhas de um arquivo:
\i /pasta/arquivo.sql

--Queries importantes para o dump caronae:

--Ver quantos usuarios existem por curso, filtrando com WHERE e AND
SELECT course, COUNT(*) FROM public.users GROUP BY course HAVING course ILIKE '%da%'
AND COUNT(*) >= 64 ORDER BY COUNT(*) DESC;

--Ver ultimos 10 pedidos de carona aceitos
SELECT * FROM ride_user WHERE status = 'accepted' ORDER BY updated_at DESC LIMIT 10;

--Ver quantos dos usuarios que entraram em 2019 ficaram mais de 6 meses usando o app
SELECT id, name, car_owner, created_at :: DATE, updated_at :: DATE FROM users
WHERE EXTRACT(YEAR FROM created_at) = '2019' AND created_at < (updated_at - INTERVAL '6 MONTHS')
ORDER BY created_at ASC;

--Ver quantos dos usuarios ficaram menos de 1 dia usando o app
SELECT id, name, created_at :: DATE, updated_at :: DATE FROM users
WHERE created_at >= (updated_at - INTERVAL '1 DAYS')
ORDER BY updated_at DESC;

--Ver quantas caronas foram feitas por dia, separadas em done = true e done = false
SELECT date :: DATE, COUNT(*), done FROM rides GROUP BY done, date :: DATE
ORDER BY date ASC;

--Ver as informacoes de uma carona especifica, e ao lado os pedidos relacionados a ela
SELECT rides.id, routine_id, slots,
rides.created_at, date, done,
ride_user.id AS request_id, ride_user.user_id, ride_user.ride_id,
ride_user.created_at, ride_user.updated_at, ride_user.status FROM rides
JOIN ride_user ON rides.id = ride_user.ride_id
WHERE rides.id = 398
ORDER BY ride_user.ride_id, request_id;

--Ver as informacoes de uma carona especifica, e ao lado os pedidos relacionados a ela
--Só para as deletadas
SELECT rides.id, routine_id, slots,
rides.created_at, date, done, deleted_at,
ride_user.id AS request_id, ride_user.user_id,
ride_user.created_at, ride_user.updated_at, ride_user.status FROM rides
JOIN ride_user ON rides.id = ride_user.ride_id
WHERE deleted_at IS NOT NULL
ORDER BY rides.id, request_id;

--Ver as informacoes de uma carona especifica, e ao lado os pedidos relacionados a ela
--De forma mais enxuta, bom para fazer análises de done e aceitos
SELECT rides.id, COUNT(*), date, done,
ride_user.created_at, ride_user.updated_at, ride_user.status FROM rides
JOIN ride_user ON rides.id = ride_user.ride_id
GROUP BY rides.id, date, done,
ride_user.created_at, ride_user.updated_at, ride_user.status
ORDER BY rides.id

--Aceitos por carona, mostrando o id e a data da carona
SELECT rides.id, COUNT(*), date, done, ride_user.status FROM rides
JOIN ride_user ON rides.id = ride_user.ride_id
GROUP BY rides.id, date, done, ride_user.status
HAVING status = 'accepted'
ORDER BY rides.id

--Aceitos por carona, ordenados por data sem hora
SELECT rides.id, COUNT(*), date :: DATE, done, ride_user.status
INTO aceitos_por_carona FROM rides
JOIN ride_user ON rides.id = ride_user.ride_id
GROUP BY rides.id, date, done, ride_user.status
HAVING status = 'accepted'
ORDER BY date, id

--Aceitos por dia
SELECT date, SUM(count), status FROM aceitos_por_data
GROUP BY status, date
ORDER BY date

--Ocupação média por dia
SELECT date, caronas, aceitos, media + 1 AS ocupacao_media FROM dados_aceitos

--Ver para cada usuário quantas vezes ele aparece no ride_user com cada status
--OBS: Toda carona criada cria um status driver, então seria muito mais preciso ver quantas caronas em que alguém
--foi aceito essa pessoa esteve de driver
SELECT user_id, COUNT(*), status FROM ride_user
WHERE status = 'driver' OR status = 'accepted'
GROUP BY user_id, status
ORDER BY user_id ASC

--Ver todo mundo que já pegou ou deu uma carona pelo menos uma vez
--OBS: Toda carona criada cria um status driver, então seria muito mais preciso ver quantas caronas em que alguém
--foi aceito essa pessoa esteve de driver
SELECT DISTINCT user_id FROM ride_user
WHERE status = 'driver' OR status = 'accepted'
ORDER BY user_id

--Ver quantas caronas cada pessoa pegou, rankeada do MVP ao zé ruela
SELECT user_id, COUNT(*) AS vezes, status FROM ride_user
WHERE status = 'accepted'
GROUP BY user_id, status
ORDER BY vezes DESC

--Criar tabela nova partir de um querie
--Bom para fazer análises mais complexas que requerem mais camadas de queries
SELECT client_id, first_name, last_name INTO new_table FROM old_table

--https://smallbusiness.chron.com/create-table-query-results-microsoft-sql-50836.html
--Esse link explica bem como criar tabelas novas a partir de queries

--status la
SELECT user_id, COUNT(*), status FROM ride_user
GROUP BY status, user_id
ORDER BY user_id

--Ver quantos usuários se cadastraram por mês
SELECT EXTRACT(MONTH FROM created_at), EXTRACT(YEAR FROM created_at), COUNT(*) FROM users
GROUP BY EXTRACT(MONTH FROM created_at), EXTRACT(YEAR FROM created_at)
ORDER BY EXTRACT(YEAR FROM created_at), EXTRACT(MONTH FROM created_at)

--Fazer um update nos não-nulos
UPDATE customers
SET favorite_website = 'techonthenet.com'
WHERE favorite_website IS NOT NULL;

SELECT COUNT(user_id), date::DATE FROM rides
JOIN ride_user ON rides.id = ride_user.ride_id
WHERE status NOT IN ('refused','quit')
GROUP BY date::DATE
ORDER BY date::DATE

--Ver cada carona, sua data, e seus pedidos agrupados por status
--Usei para criar a tabela "aa_status_por_carona"
SELECT rides.id, date :: DATE, COUNT(*), ride_user.status FROM rides
JOIN ride_user ON rides.id = ride_user.ride_id
GROUP BY rides.id, date, ride_user.status
ORDER BY rides.id

--Ver cada carona, sua data, seus pedidos e o usuário relacionado ao pedido
--Usei para criar a tabela "aa_pedidos_por_carona"
SELECT rides.id, date :: DATE, ride_user.user_id, ride_user.status FROM rides
JOIN ride_user ON rides.id = ride_user.ride_id
GROUP BY rides.id, date, ride_user.user_id, ride_user.status
ORDER BY rides.id

--Ranking dos motoristas que mais deram caronas que tiveram alguém aceito ou pendente na carona
SELECT b.user_id, COUNT(DISTINCT a.ride_id) AS coun FROM 
(SELECT * FROM ride_user 
WHERE status = 'pending' OR status = 'accepted') a
LEFT JOIN 
(SELECT * FROM ride_user 
WHERE status = 'driver') b
ON a.ride_id = b.ride_id
GROUP BY b.user_id
ORDER BY coun DESC

--Ver os pedidos (pending e accepted) de cada carona junto com quem pediu, e ao lado quem era o motorista
SELECT * FROM 
(SELECT * FROM ride_user 
WHERE status = 'pending' OR status = 'accepted') caronistas
LEFT JOIN 
(SELECT * FROM ride_user 
WHERE status = 'driver') motoristas
ON caronistas.ride_id = motoristas.ride_id
ORDER BY caronistas.ride_id

--Ver os pedidos (pending e accepted) de cada carona junto com quem pediu, e ao lado quem era o motorista (pedidosXdriver)
--Aqui eu mudei os nomes das colunas da primeira metade para não terem duas colunas com o mesmo nome pois o Power BI não aceita
SELECT * FROM 
(SELECT id AS id_pedcar, user_id AS user_id_car, ride_id AS ride_id_1,
created_at AS created_at_pedcar, updated_at AS updated_at_pedcar, status AS status_car
FROM ride_user 
WHERE status = 'pending' OR status = 'accepted') caronistas
LEFT JOIN 
(SELECT * FROM ride_user 
WHERE status = 'driver') motoristas
ON caronistas.ride_id_1 = motoristas.ride_id
ORDER BY caronistas.ride_id_1

--Ver todos os pedidos (driver, accepted e pending) referentes a caronas válidas (caronas com algum accepted ou pending)
SELECT * FROM ride_user
WHERE ride_id IN (
	SELECT DISTINCT ride_id FROM ride_user
	WHERE status = 'accepted' OR status = 'pending'
	ORDER BY ride_id
)
AND status IN ('driver','pending','accepted')
ORDER BY ride_id

--Filtrar por comprimento da palavra
WHERE LENGTH(profile) > 5

--Pega os usuários que estiveram envolvidos em caronas que foram ou voltaram de/para o Centro
SELECT DISTINCT user_id FROM ride_user
WHERE ride_id IN (
	SELECT id FROM rides
	WHERE myzone = 'Centro')

--Caronas válidas por dia
SELECT date, COUNT(DISTINCT id) FROM aa_status_por_carona
WHERE status = 'pending' OR status = 'accepted'
GROUP BY date
ORDER BY date

--Pessoas afetadas por dia (que tiveram status driver accepted ou pending)
SELECT date::DATE, COUNT(DISTINCT user_id) FROM rides
JOIN ride_user ON rides.id = ride_user.ride_id
WHERE status NOT IN ('refused','quit')
GROUP BY date::DATE
ORDER BY date::DATE

--Usei para deletar os pedidos referentes a caronas deletadas
DELETE FROM ride_user
WHERE id IN (
	SELECT ride_user.id AS request_id
	FROM rides
	JOIN ride_user ON rides.id = ride_user.ride_id
	WHERE deleted_at IS NOT NULL
)

--Ver quais são os modelos de carros mais populares
SELECT car_model, COUNT(*) AS quantos FROM users
--WHERE car_owner = true
GROUP BY car_model
ORDER BY quantos DESC

--Pegar dados sobre as pessoas envolvidas em uma carona específica
SELECT id, name, car_owner, car_model, location FROM users
WHERE id IN
	(SELECT user_id FROM ride_user
	WHERE ride_id = 5436)

--Ver todas as caronas dadas por usuários que tem Palio (caronas executadas com um Palio)
SELECT * FROM rides
WHERE id IN
	(SELECT ride_id FROM ride_user
	WHERE status = 'driver'
	AND user_id IN
		(SELECT id FROM users
		WHERE car_model = 'Palio'))
ORDER BY id ASC

--Ver todas as caronas pegadas por um usuário, com o bairro de origem/destino e distância para o fundão de cada uma
SELECT ride_user.id, ride_user.user_id, ride_id, ride_user.created_at,
neighborhood, distance FROM ride_user
JOIN rides ON rides.id = ride_user.ride_id
JOIN neighborhoods ON name = rides.neighborhood
WHERE user_id = 1762 AND (status = 'pending' OR status = 'accepted')
ORDER BY id ASC

--Total de kilometros percorridos por caronas em que cada pessoa foi de caronista
SELECT ride_user.user_id, SUM(distance) AS kilometragem FROM ride_user
JOIN rides ON rides.id = ride_user.ride_id
JOIN neighborhoods ON name = rides.neighborhood
WHERE status = 'pending' OR status = 'accepted'
GROUP BY user_id
ORDER BY kilometragem DESC

--Total de kilometros percorridos por caronas em que cada pessoa foi de caronista, e o modelo do seu carro ao lado, se houver
--A linha comentada permite ver apenas os que têm modelo de carro listado
SELECT ride_user.user_id, SUM(distance) AS kilometragem, car_model FROM ride_user
JOIN rides ON rides.id = ride_user.ride_id
JOIN neighborhoods ON name = rides.neighborhood
JOIN users ON users.id = ride_user.user_id
WHERE (status = 'pending' OR status = 'accepted')
--AND LENGTH(car_model) > 1
GROUP BY user_id, car_owner, car_model
ORDER BY kilometragem DESC

--Total de kilometros percorridos por caronas em que cada pessoa foi de caronista de/para o fundão,
-- e o modelo do seu carro ao lado
SELECT ride_user.user_id, SUM(distance_fundao) AS km_fundao, car_model
FROM ride_user
JOIN rides ON rides.id = ride_user.ride_id
JOIN neighborhoods ON name = rides.neighborhood
JOIN users ON users.id = ride_user.user_id
WHERE (status = 'pending' OR status = 'accepted')
AND (rides.hub ILIKE '%CT%' OR rides.hub ILIKE '%CCMN%' OR rides.hub ILIKE '%CCS%' OR rides.hub ILIKE '%Letras%'
	 OR rides.hub ILIKE '%Reitoria%' OR rides.hub ILIKE '%EEFD%')
AND LENGTH(car_model) > 1
GROUP BY user_id, car_model
ORDER BY km_fundao DESC

--Ver as caronas nas quais as pessoas com carro foram pending ou accepted, filtrando pelo hub
SELECT rides.id, user_id, status, hub, neighborhoods.name, hub, distance_pv FROM ride_user
JOIN rides ON rides.id = ride_user.ride_id
JOIN neighborhoods ON name = rides.neighborhood
JOIN users ON users.id = ride_user.user_id
WHERE LENGTH(car_model) > 1
AND (status = 'pending' OR status = 'accepted')
AND hub ILIKE '%PRAIA VERMELHA%'
ORDER BY rides.id

--Ver distâncias das caronas de alguém específico
SELECT user_id, status, hub, neighborhoods.name, distance, distance_fundao FROM ride_user
JOIN rides ON rides.id = ride_user.ride_id
JOIN neighborhoods ON name = rides.neighborhood
JOIN users ON users.id = ride_user.user_id
WHERE user_id = 7649
AND (status = 'pending' OR status = 'accepted')

--Ver bairros das caronas em que as pessoas que tem carro foram de caronistas
SELECT DISTINCT neighborhood FROM ride_user
JOIN rides ON rides.id = ride_user.ride_id
JOIN neighborhoods ON name = rides.neighborhood
JOIN users ON users.id = ride_user.user_id
WHERE (status = 'pending' OR status = 'accepted')
AND LENGTH(car_model) > 1
ORDER BY neighborhood ASC

--Aba mudança
SELECT * FROM rides
WHERE neighborhood ILIKE '%alameda%'
--AND myzone = 'Outros'

UPDATE rides SET myzone = 'Grande Niterói'
WHERE neighborhood ILIKE '%alameda%'

UPDATE rides SET neighborhood = 'São Cristóvão'
WHERE neighborhood ILIKE '%são cristóvão%'

--Aba adicionar bairros
SELECT * FROM neighborhoods
ORDER BY id ASC

INSERT INTO neighborhoods (
	name,
	distance,
	zone_id)
VALUES ('Xerém',37.7,1)

--Ver os status de caronas dos usuários car_owner = false com modelo do carro mas sem placa ou cor do carro
SELECT users.id, name, car_owner, car_model, car_color, car_plate, ride_id, status FROM users
JOIN ride_user ON ride_user.user_id = users.id 
WHERE car_owner = false
AND LENGTH(car_model) > 1
AND (LENGTH(car_color) < 1 OR LENGTH(car_plate) < 7)
GROUP BY users.id, name, car_owner, car_model, car_color, car_plate, ride_id, status
ORDER BY users.id

--Fazer um update dos dados de um bairro
UPDATE neighborhoods SET distance_fundao = 11.0, distance_pv = 24.7, distance_macae = 194 WHERE name = 'Abolição';

--Adicionar colunas a uma tabela
ALTER TABLE neighborhoods
ADD distance_macae NUMERIC(8,2)

--- 06/2022 ---

--Ocupação, distância e pessoas km das caronas indo/voltando do/pro fundao
SELECT rides.id, (COUNT(status)+1) as pessoas, date, neighborhood, hub, distance_fundao,
	(distance_fundao * (COUNT(status)+1)) as pessoas_km FROM rides
JOIN ride_user ON rides.id = ride_user.ride_id
JOIN neighborhoods ON name = rides.neighborhood
WHERE (status = 'pending' OR status = 'accepted')
AND (rides.hub ILIKE '%CT%' OR rides.hub ILIKE '%CCMN%' OR rides.hub ILIKE '%CCS%' OR rides.hub ILIKE '%Letras%'
	 OR rides.hub ILIKE '%Reitoria%' OR rides.hub ILIKE '%EEFD%')
GROUP BY rides.id, date, done, neighborhood, hub, distance_fundao
ORDER BY rides.id

--Soma de pessoas km total de caronas indo/voltando do/pro fundao
SELECT SUM(pessoas_km) FROM (
	SELECT rides.id, (COUNT(status)+1) as pessoas, date, neighborhood, hub, distance_fundao,
		(distance_fundao * (COUNT(status)+1)) as pessoas_km FROM rides
	JOIN ride_user ON rides.id = ride_user.ride_id
	JOIN neighborhoods ON name = rides.neighborhood
	WHERE (status = 'pending' OR status = 'accepted')
	AND (rides.hub ILIKE '%CT%' OR rides.hub ILIKE '%CCMN%' OR rides.hub ILIKE '%CCS%' OR rides.hub ILIKE '%Letras%'
		 OR rides.hub ILIKE '%Reitoria%' OR rides.hub ILIKE '%EEFD%')
	GROUP BY rides.id, date, done, neighborhood, hub, distance_fundao
	ORDER BY rides.id) a