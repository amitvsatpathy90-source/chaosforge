-- Runs once at first Postgres init (mounted into /docker-entrypoint-initdb.d/).
-- ChaosForge uses two logical databases (architecture specifications): chaosforge_cp and chaosforge_exec.
-- Flyway V1–V6 manages the schema WITHIN each database at the owning service's startup —
-- this script only creates the empty databases.
CREATE DATABASE chaosforge_cp;
CREATE DATABASE chaosforge_exec;

-- Owned by the same lab role created via POSTGRES_USER. Tighten roles before any real deployment.
GRANT ALL PRIVILEGES ON DATABASE chaosforge_cp   TO chaosforge;
GRANT ALL PRIVILEGES ON DATABASE chaosforge_exec TO chaosforge;
