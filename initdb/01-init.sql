CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD '1234';

GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- 1) Colocar o DB sob o dono 'airflow'
ALTER DATABASE airflow OWNER TO airflow;

-- 2) Garantir acesso ao DB
GRANT CONNECT, TEMP ON DATABASE airflow TO airflow;

-- 3) Dar permissão no schema public
GRANT USAGE, CREATE ON SCHEMA public TO airflow;

-- 4) Transferir a posse do schema public para airflow
ALTER SCHEMA public OWNER TO airflow;