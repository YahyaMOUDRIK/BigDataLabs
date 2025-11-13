USE hotel_booking;

LOAD DATA LOCAL INPATH '/shared_volume/hotel_data/clients.txt' INTO TABLE clients;
LOAD DATA LOCAL INPATH '/shared_volume/hotel_data/hotels.txt' INTO TABLE hotels;
LOAD DATA LOCAL INPATH '/shared_volume/hotel_data/reservations.txt' INTO TABLE reservations;

INSERT INTO TABLE hotels_partitioned PARTITION (ville)
SELECT hotel_id, nom, etoiles, ville FROM hotels;

INSERT INTO TABLE reservations_bucketed
SELECT * FROM reservations;