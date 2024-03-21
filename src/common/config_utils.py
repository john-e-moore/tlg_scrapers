import yaml

################################################################################
# 
################################################################################
def load_config(config_path: str) -> dict:
    """Load config from yml file."""
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except yaml.YAMLError as exc:
        print(f"Error in configuration file:\n{exc}")