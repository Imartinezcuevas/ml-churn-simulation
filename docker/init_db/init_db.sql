CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR PRIMARY KEY,
    signup_date TIMESTAMP,
    email VARCHAR,
    region VARCHAR,
    plan VARCHAR,
    device VARCHAR
);

CREATE TABLE IF NOT EXISTS activity (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR REFERENCES customers(customer_id),
    session_start TIMESTAMP,
    session_length FLOAT,
    device VARCHAR
);

CREATE TABLE IF NOT EXISTS support (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR REFERENCES customers(customer_id),
    ticket_date TIMESTAMP,
    issue_type VARCHAR,
    resolved BOOLEAN 
);

CREATE TABLE IF NOT EXISTS billing (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR REFERENCES customers(customer_id),
    payment_date TIMESTAMP,
    amount FLOAT,
    status VARCHAR
);