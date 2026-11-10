from alembic import context

from orchestrator_service.models import Base
from vp_common.migrations import run_env

run_env(context, Base.metadata)
