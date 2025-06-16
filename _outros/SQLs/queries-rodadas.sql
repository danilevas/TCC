-- Usei para deletar os pedidos referentes a caronas deletadas
DELETE FROM ride_user
WHERE id IN (
	SELECT ride_user.id AS request_id
	FROM rides
	JOIN ride_user ON rides.id = ride_user.ride_id
	WHERE deleted_at IS NOT NULL
)

-- Cria zona do Fundão
INSERT INTO zones (
	name,
	color)
VALUES ('Ilha do Fundão','#1DA75D')

UPDATE zones SET color = '#5A2817'
WHERE name = 'Ilha do Fundão'

-- Cria zona da Região Centro-Sul Fluminense (criei porque tinha um neighborhood "Vassouras" em rides)
INSERT INTO zones (
	name,
	color)
VALUES ('Região Centro-Sul','#1DA75D')

-- Alterações de zonas de bairros que estavam com myzone = "Outros" na tabela rides
-- O número embaixo é a quantidade de linhas alteradas

UPDATE rides SET myzone = 'Rio das Ostras'
WHERE neighborhood ILIKE '%rio das ostras%'
68

UPDATE rides SET myzone = 'Região Serrana'
WHERE neighborhood ILIKE '%petrópolis%'
OR neighborhood ILIKE '%teresópolis%'
46

UPDATE rides SET myzone = 'Grande Niterói'
WHERE neighborhood ILIKE '%fonseca%'
OR neighborhood ILIKE '%niterói%'
3811

UPDATE rides SET myzone = 'Zona Norte'
WHERE neighborhood ILIKE '%ilha do governador%'
178

UPDATE rides SET myzone = 'Grande Niterói'
WHERE neighborhood ILIKE '%icaraí%'
353

UPDATE rides SET myzone = 'Ilha do Fundão'
WHERE neighborhood ILIKE '%incubadora%'
19

UPDATE rides SET myzone = 'Zona Norte'
WHERE neighborhood ILIKE '%são cristóvão%'
OR neighborhood ILIKE '%sao cristovao%'
489

UPDATE rides SET myzone = 'Zona Norte'
WHERE neighborhood ILIKE '%fiocruz%'
10

UPDATE rides SET myzone = 'Centro'
WHERE neighborhood ILIKE '%bairro de fátima%'
27

UPDATE rides SET myzone = 'Zona Sul'
WHERE neighborhood ILIKE '%UFRJ DA PRAIA VERMELHA%'
13

UPDATE rides SET myzone = 'Ilha do Fundão'
WHERE neighborhood ILIKE '%ufrj letras%'
1

UPDATE rides SET myzone = 'Baixada'
WHERE neighborhood ILIKE '%XERÉM%'
136

UPDATE rides SET myzone = 'Zona Oeste'
WHERE neighborhood ILIKE '%jacarepaguá%'
1584

UPDATE rides SET myzone = 'Grande Niterói'
WHERE neighborhood ILIKE '%são gonçalo%'
OR neighborhood ILIKE '%maria paula%'
1886

UPDATE rides SET myzone = 'Região Serrana'
WHERE neighborhood ILIKE '%Eng. Paulo de Frontin%'
8

UPDATE rides SET myzone = 'Zona Oeste'
WHERE neighborhood ILIKE '%Praça Seca%'
184

UPDATE rides SET myzone = 'Região dos Lagos'
WHERE neighborhood ILIKE '%unamar%'
109

UPDATE rides SET myzone = 'Zona Norte'
WHERE neighborhood ILIKE '%jardim guanabara%'
1491

UPDATE rides SET myzone = 'Grande Niterói'
WHERE neighborhood ILIKE '%maricá%'
10

UPDATE rides SET myzone = 'Região dos Lagos'
WHERE neighborhood ILIKE 'arraial do cabo%'
28

UPDATE rides SET myzone = 'Grande Niterói'
WHERE neighborhood ILIKE '%itaipuaçu%'
9

UPDATE rides SET myzone = 'Zona Oeste'
WHERE neighborhood ILIKE '%barra da tijuca%'
5250

UPDATE rides SET myzone = 'Zona Oeste'
WHERE neighborhood ILIKE 'pechincha'
633

UPDATE rides SET myzone = 'Grande Niterói'
WHERE neighborhood ILIKE 'alameda'
OR neighborhood ILIKE 'ingá'
4

UPDATE rides SET myzone = 'Região dos Lagos'
WHERE neighborhood ILIKE 'Cabo Frio'
105

UPDATE rides SET myzone = 'Baixada'
WHERE neighborhood ILIKE '%nova iguaçu%'
986

UPDATE rides SET myzone = 'Grande Niterói'
WHERE neighborhood ILIKE 'Santa Rosa'
2

UPDATE rides SET myzone = 'Zona Sul'
WHERE neighborhood ILIKE 'UFRJ PRAIA VERMELHA'
1

UPDATE rides SET myzone = 'Ilha do Fundão'
WHERE neighborhood ILIKE '%fundão%'
2

UPDATE rides SET myzone = 'Região Centro-Sul'
WHERE neighborhood ILIKE 'VASSOURAS'
1

UPDATE rides SET myzone = 'Baixada'
WHERE neighborhood ILIKE '%belford roxo%'
168

UPDATE rides SET myzone = 'Zona Sul'
WHERE neighborhood ILIKE 'flamengo'
AND myzone = 'Outros'
1

UPDATE rides SET myzone = 'Região dos Lagos'
WHERE neighborhood ILIKE 'São Pedro da Aldeia'
AND myzone = 'Outros'
1

UPDATE rides SET myzone = 'Zona Norte'
WHERE neighborhood ILIKE '%ilha do goverador%'
1

UPDATE rides SET myzone = 'Zona Oeste'
WHERE neighborhood ILIKE 'recreio'
5

UPDATE rides SET myzone = 'Grande Niterói'
WHERE neighborhood ILIKE '%Santa Rosa%'
21

UPDATE rides SET myzone = 'Zona Sul'
WHERE neighborhood ILIKE 'Glória '
1

UPDATE rides SET myzone = 'Grande Niterói'
WHERE neighborhood ILIKE 'Pita-SG'
1

UPDATE rides SET myzone = 'Zona Norte'
WHERE neighborhood ILIKE 'UERJ'
1

UPDATE rides SET myzone = 'Zona Norte'
WHERE neighborhood ILIKE 'Maria da Graça'
3

-- Deleta 13 caronas com neighborhood "teste" ou "test"
DELETE FROM rides
WHERE neighborhood ILIKE 'teste'
OR neighborhood ILIKE 'test'
13

-- Seta car_owner = True para os usuários com car_owner=False,
-- porém com dado de modelo, cor e placa (pelo menos um caracter em cada um desses campos)
UPDATE users SET car_owner = true 
WHERE car_owner = false
AND LENGTH(car_model) > 1
AND LENGTH(car_color) > 1
AND LENGTH(car_plate) > 1
67

-- Seta car_owner = True para os usuários com car_owner=False,
-- porém com dado de modelo e cor (pelo menos um caracter em cada um desses campos), independente de ter placa ou não 
UPDATE users SET car_owner = true
WHERE car_owner = false
AND (
	LENGTH(car_model) > 1
	AND LENGTH(car_color) > 1
	-- OR LENGTH(car_plate) > 1
)
33

-- Seta car_owner = True para os usuários com car_owner=False,
-- porém com dado de modelo e placa (pelo menos um caracter em cada um desses campos), independente de ter cor ou não 
UPDATE users SET car_owner = true
WHERE car_owner = false
AND (
	LENGTH(car_model) > 1
	-- OR LENGTH(car_color) > 1
	AND LENGTH(car_plate) > 1
)
1

-- Seta car_owner = True para os usuários com car_owner=False,
-- porém com dado de modelo ou placa válida 
UPDATE users SET car_owner = true
WHERE car_owner = false
AND (
	LENGTH(car_model) >= 1
	OR LENGTH(car_plate) >= 7
)

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