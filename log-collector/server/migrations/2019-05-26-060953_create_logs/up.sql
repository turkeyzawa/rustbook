-- Your SQL goes here

CREATE Table logs (
    id BIGSERIAL NOT NULL,
    user_agent VARCHAR NOT NULL,
    response_time INT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (id)
);