-- Separate database for the integration test suite so tests never touch dev data.
CREATE DATABASE notifications_test OWNER notifications;
