CREATE DATABASE aispy;
USE aispy;

// CAMERA DETECTIONS

DROP TABLE camera_detections;
TRUNCATE TABLE camera_detections;
CREATE TABLE camera_detections (
    id bigint(11) NOT NULL AUTO_INCREMENT,
    feature_vector varbinary(4096) NOT NULL,
    camera varchar(50) NOT NULL,
    ts int NOT NULL,
    checked smallint(1) NOT NULL DEFAULT FALSE,
    KEY id (id) USING CLUSTERED COLUMNSTORE
);

DROP PIPELINE face_pipeline;
CREATE AGGREGATOR PIPELINE face_pipeline AS
LOAD DATA KAFKA 'kafka:9093/camera_detections'
INTO TABLE `camera_detections`
 (@raw_encoding <- encoding,
  camera <- camera,
  ts <- ts
)
FORMAT JSON
SET feature_vector = json_array_pack(@raw_encoding);

PROFILE PIPELINE face_pipeline;
TEST PIPELINE face_pipeline LIMIT 100;
START PIPELINE face_pipeline;


// FACE REGISTRATIONS

DROP TABLE face_registrations;
TRUNCATE TABLE face_registrations;
CREATE TABLE face_registrations (
    id bigint(11) NOT NULL AUTO_INCREMENT,
    feature_vector varbinary(4096) NOT NULL,
    username varchar(50) NOT NULL,
    KEY id (id) USING CLUSTERED COLUMNSTORE
);

DROP PIPELINE registrations_pipeline;
CREATE AGGREGATOR PIPELINE registrations_pipeline AS
LOAD DATA KAFKA 'kafka:9093/face_registrations'
INTO TABLE `face_registrations`
 (@raw_encoding <- encoding,
  username <- username
)
FORMAT JSON
SET feature_vector = json_array_pack(@raw_encoding);

PROFILE PIPELINE registrations_pipeline;
TEST PIPELINE registrations_pipeline LIMIT 1;
START PIPELINE registrations_pipeline;

DROP VIEW faces_with_confidence;
CREATE VIEW faces_with_confidence AS
  SELECT c.id, f.id AS face_id, f.username AS username, dot_product(c.feature_vector, f.feature_vector) AS confidence
  FROM camera_detections AS c
  JOIN face_registrations AS f ON dot_product(c.feature_vector, f.feature_vector) > 0.9;

CREATE VIEW tagged_faces AS
  SELECT c.id AS id, c.camera As camera, c.ts AS ts, faces.face_id AS face_id, faces.username AS username
  FROM camera_detections AS c
  JOIN (
    SELECT MAX(id) as id, MAX(confidence) as confidence
    FROM faces_with_confidence GROUP BY (id)
  ) AS max_confidence ON (max_confidence.id = c.id)
  JOIN faces_with_confidence AS faces ON (faces.id = c.id AND faces.confidence = max_confidence.confidence);


SELECT * FROM tagged_faces ORDER BY id DESC LIMIT 5000;