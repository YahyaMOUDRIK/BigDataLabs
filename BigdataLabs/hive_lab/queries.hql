USE hotel_booking;

SELECT * FROM clients;

SELECT * FROM hotels WHERE ville = 'Paris';

SELECT 
  r.reservation_id,
  c.nom as client_nom,
  h.nom as hotel_nom,
  r.date_debut,
  r.date_fin,
  r.prix_total
FROM reservations r
JOIN clients c ON r.client_id = c.client_id
JOIN hotels h ON r.hotel_id = h.hotel_id;

SELECT 
  c.nom,
  COUNT(r.reservation_id) as nb_reservations
FROM clients c
LEFT JOIN reservations r ON c.client_id = r.client_id
GROUP BY c.client_id, c.nom;

SELECT 
  h.nom,
  COUNT(r.reservation_id) as nb_reservations
FROM hotels h
JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.hotel_id, h.nom
HAVING COUNT(r.reservation_id) > 1;

SELECT 
  h.nom
FROM hotels h
LEFT JOIN reservations r ON h.hotel_id = r.hotel_id
WHERE r.reservation_id IS NULL;

SELECT DISTINCT c.nom
FROM clients c
WHERE c.client_id IN (
  SELECT r.client_id
  FROM reservations r
  JOIN hotels h ON r.hotel_id = h.hotel_id
  WHERE h.etoiles > 4
);

SELECT 
  h.nom,
  SUM(r.prix_total) as revenus_total
FROM hotels h
JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.hotel_id, h.nom
ORDER BY revenus_total DESC;

SELECT 
  hp.ville,
  SUM(r.prix_total) as revenus_total
FROM reservations r
JOIN hotels_partitioned hp ON r.hotel_id = hp.hotel_id
GROUP BY hp.ville;

SELECT 
  c.nom,
  COUNT(rb.reservation_id) as nb_reservations
FROM clients c
LEFT JOIN reservations_bucketed rb ON c.client_id = rb.client_id
GROUP BY c.client_id, c.nom;