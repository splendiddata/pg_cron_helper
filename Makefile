EXTENSION = pg_cron_helper
DATA = pg_cron_helper--0.1.sql pg_cron_helper--0.1--0.2.sql
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

