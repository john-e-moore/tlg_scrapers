
################################################################################
# 
################################################################################
def clean_text(text):
    """Apply to all cells of a DataFrame to remove non-ASCII and leading/trailing spaces."""
    if isinstance(text, str):
        return ''.join(char for char in text if ord(char) < 128).strip()
    
    return text