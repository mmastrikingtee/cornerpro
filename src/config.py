import os
def as_bool(v, default=False):
    if v is None: return default
    return str(v).lower() in {"1","true","yes","y","on"}
DATABASE_URL = os.getenv("DATABASE_URL","sqlite:///./data/cornerpro.sqlite")
USE_FIXTURES = as_bool(os.getenv("USE_FIXTURES"), True)
