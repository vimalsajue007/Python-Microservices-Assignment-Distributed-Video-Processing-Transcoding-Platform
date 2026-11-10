from alembic import context

from upload_service.models import Base
from vp_common.migrations import run_env

run_env(context, Base.metadata)
