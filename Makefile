EXTENSION = pg_cron_helper
DATA = pg_cron_helper--0.1.sql
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

