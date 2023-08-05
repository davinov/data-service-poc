CREATE TABLE sample_data (
    user_id          varchar(80),
    value            int
);

INSERT INTO sample_data (user_id, value) VALUES
    ('A', 1),
    ('B', 2),
    ('C', 3)
;

CREATE TABLE users (
    user_id         varchar(80),
    user_name       varchar(80)
);

INSERT INTO users (user_id, user_name) VALUES
    ('A', 'Anthony'),
    ('B', 'Bérénice'),
    ('C', 'Charlotte')
;
